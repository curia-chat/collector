import os
import re
import openai
from openai import OpenAI
from openai import OpenAIError
from openai import APIError
from qdrant_client import QdrantClient
from qdrant_client.http import models
from settings import get_mysql_connection, OPENAI_API_KEY, QDRANT_API_KEY, QDRANT_URL
from datetime import datetime

# Initialize OpenAI API key
#openai.api_key = OPENAI_API_KEY
openai = OpenAI(api_key=OPENAI_API_KEY)

# Initialize Qdrant client
qdrant_client = QdrantClient(
    url=QDRANT_URL,
    port=443,
    api_key=QDRANT_API_KEY,
)

# Collection name and vector size
collection_name = "judgments_embeddings_new"
vector_size = 1536  # As per text-embedding-3-small

# Function to create or update collection with indexing configurations
def setup_qdrant_collection():
    # Check if the collection exists
    collection_exists = False
    try:
        qdrant_client.get_collection(collection_name)
        collection_exists = True
        print(f"Collection '{collection_name}' already exists in Qdrant.")
    except Exception as e:
        print(f"Collection '{collection_name}' does not exist.")

    if not collection_exists:
        print(f"Creating collection '{collection_name}'.")
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE)
        )

    # Define payload fields to index
    fields_to_index = ["metadata.case_no", "metadata.ECLI", "metadata.date_decided"]
    for field_name in fields_to_index:
        try:
            qdrant_client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema="keyword"  # Correctly specify the field schema
            )
            print(f"Created index for field '{field_name}' as 'keyword'.")
        except Exception as e:
            print(f"Index for field '{field_name}' may already exist or an error occurred: {e}")

# Function to check if a judgment has been processed
def is_judgment_processed(cursor, judgment_id):
    query = "SELECT 1 FROM judgments_upsert WHERE judgment_id = %s LIMIT 1"
    cursor.execute(query, (judgment_id,))
    return cursor.fetchone() is not None

# Function to mark a judgment as processed
def mark_judgment_processed(cursor, judgment_id):
    query = "INSERT INTO judgments_upsert (judgment_id) VALUES (%s)"
    cursor.execute(query, (judgment_id,))

import re

def split_text_with_linebreaks(text, max_length=2000, overlap=200):
    """
    Splits the input text into chunks with specified maximum length and overlap.
    Ensures that the overlap contains at least two complete sentences.

    Parameters:
        text (str): The input text to split.
        max_length (int): The maximum length of each chunk.
        overlap (int): The number of characters to overlap between chunks.

    Returns:
        List[str]: A list of text chunks.
    """
    lines = text.split('\n')
    chunks = []
    current_chunk = ''
    i = 0

    # Regex to identify sentence endings: '.', '!', '?', possibly followed by quotes or parentheses
    sentence_end_pattern = re.compile(r'[.!?]["\')\]]*\s')

    while i < len(lines):
        line = lines[i]
        line_length = len(line)

        if line_length == 0:
            i += 1
            continue

        # Wenn die Zeile länger als max_length ist, splitte die Zeile
        if line_length > max_length:
            start = 0
            while start < line_length:
                end = start + max_length
                chunk = line[start:end]
                chunks.append(chunk.strip())
                start += max_length - overlap
            i += 1
            continue

        # Versuch, die Zeile zum aktuellen Chunk hinzuzufügen
        if len(current_chunk) + line_length + 1 <= max_length:
            if current_chunk:
                current_chunk += '\n' + line
            else:
                current_chunk = line
        else:
            # Aktuellen Chunk hinzufügen
            if current_chunk:
                chunks.append(current_chunk.strip())

                # Überlappung berechnen: mindestens zwei vollständige Sätze
                if overlap > 0 and len(current_chunk) > overlap:
                    overlap_text_candidate = current_chunk[-overlap:]
                    # Suche alle Satzenden innerhalb des Overlap-Kandidaten
                    matches = list(sentence_end_pattern.finditer(overlap_text_candidate))
                    
                    if len(matches) >= 2:
                        # Position nach dem zweiten letzten Satzende
                        second_last_match = matches[-2]
                        overlap_start = second_last_match.end()
                        overlap_text = overlap_text_candidate[overlap_start:].strip()
                    elif len(matches) == 1:
                        # Nur ein Satzende gefunden
                        last_match = matches[-1]
                        overlap_start = last_match.end()
                        overlap_text = overlap_text_candidate[overlap_start:].strip()
                    else:
                        # Kein Satzende gefunden, suche nach dem letzten Leerzeichen
                        last_space = overlap_text_candidate.rfind(' ')
                        if last_space != -1:
                            overlap_text = overlap_text_candidate[last_space:].strip()
                        else:
                            # Kein Leerzeichen gefunden, nimm die gesamte Overlap-Kandidat
                            overlap_text = overlap_text_candidate.strip()
                elif len(current_chunk) <= overlap:
                    # Aktueller Chunk ist kleiner als overlap, nutze den gesamten Chunk
                    overlap_text = current_chunk.strip()
                else:
                    # Nicht genügend Zeichen für Overlap
                    overlap_text = ''

                # Neuen Chunk mit Overlap starten
                if overlap_text:
                    current_chunk = overlap_text + '\n' + line
                else:
                    current_chunk = line
            else:
                current_chunk = line

        i += 1

    # Letzten Chunk hinzufügen, falls noch nicht abgeschlossen
    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks

def process_judgments():
    print("Starting judgment processing...")
    # Set up Qdrant collection and indexing
    setup_qdrant_collection()

    # Connect to MySQL database
    conn = get_mysql_connection()
    print("Verbindung erfolgreich hergestellt!")

    try:
        with conn.cursor() as cursor:
            # Ensure upsert table exists with processed_at column
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS judgments_upsert (
                    judgment_id BIGINT PRIMARY KEY,
                    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("Checked/Created judgments_upsert table.")

            # Fetch judgments where text_summary_de is not null and not yet processed
            query = """
                SELECT id, ecli, case_no, date_decided, text_de, text_summary_de
                FROM Judgments
                WHERE text_summary_de IS NOT NULL
                AND text_de IS NOT NULL
                AND id NOT IN (SELECT judgment_id FROM judgments_upsert)
                LIMIT 5000
            """
            cursor.execute(query)
            judgments = cursor.fetchall()
            print(f"Fetched {len(judgments)} judgments to process.")

            for judgment in judgments:
                try:
                    judgment_id = judgment[0]
                    ecli = judgment[1]
                    case_no = judgment[2]
                    date_decided = judgment[3]
                    text_de = judgment[4]
                    text_summary_de = judgment[5]

                    print(f"\nProcessing judgment ID: {judgment_id}, Case Number (Aktenzeichen): {case_no}")

                    # Check if already processed
                    if is_judgment_processed(cursor, judgment_id):
                        print(f"Judgment {judgment_id} already processed. Skipping.")
                        continue

                    if not text_de:
                        print(f"No text_de for judgment {judgment_id}. Skipping.")
                        continue

                    # Split text_de into chunks with overlap using line breaks
                    print("Splitting text into chunks...")
                    chunks = split_text_with_linebreaks(text_de, max_length=1500, overlap=200)
                    #chunks = split_text_by_paragraphs(text_de, max_length=2000, overlap=200)
                    print(f"Text split into {len(chunks)} chunks.")

                    # Prepare data for embeddings
                    embeddings_data = []
                    for idx, chunk in enumerate(chunks):
                        embeddings_data.append({
                            'judgment_id': judgment_id,
                            'chunk': chunk,
                            'chunk_idx': idx
                        })

                    # Process embeddings in batches of 20
                    batch_size = 20
                    for i in range(0, len(embeddings_data), batch_size):
                        batch = embeddings_data[i:i+batch_size]
                        texts = [item['chunk'] for item in batch]

                        # Generate embeddings
                        try:
                            print(f"Generating embeddings for batch {i//batch_size + 1}...")
                            response = openai.embeddings.create(
                                input=texts,
                                model="text-embedding-3-small"
                            )
                        except OpenAIError as e:
                            print(f"OpenAI API error while processing batch {i//batch_size + 1} for judgment {judgment_id}: {e}")
                            continue  # Skip this batch on error

                        # Access embeddings from the response
                        embeddings = [data.embedding for data in response.data]

                        # Prepare points for Qdrant
                        points = []
                        for emb_data, embedding in zip(batch, embeddings):
                            payload = {
                                'Content': emb_data['chunk'],  # Not indexed as full-text
                                'metadata': {
                                    'case_no': case_no,
                                    'ECLI': ecli,
                                    'date_decided': date_decided.strftime('%Y-%m-%d') if date_decided else None,
                                   # 'text_summary_de': text_summary_de
                                },
                                'loc': {
                                    'judgment_id': emb_data['judgment_id'],
                                    'chunk_idx': emb_data['chunk_idx']
                                }
                            }
                            #point_id = f"{emb_data['judgment_id']}{emb_data['chunk_idx']}"
                            point_id = emb_data['judgment_id'] * 10000 + emb_data['chunk_idx']
                            points.append(models.PointStruct(
                                id=point_id,
                                vector=embedding,
                                payload=payload
                            ))

                        # Upload to Qdrant
                        try:
                            print(f"Uploading batch {i//batch_size + 1} to Qdrant...")
                            qdrant_client.upsert(
                                collection_name=collection_name,
                                points=points
                            )
                        except Exception as e:
                            print(f"Qdrant upsert error while uploading batch {i//batch_size + 1} for judgment {judgment_id}: {e}")
                            continue  # Skip this batch on error

                        print(f"Uploaded batch {i//batch_size + 1} for judgment {judgment_id}")

                    # Mark judgment as processed
                    mark_judgment_processed(cursor, judgment_id)
                    conn.commit()
                    print(f"Judgment {judgment_id} processing complete.")

                except Exception as e:
                    print(f"An error occurred while processing judgment ID {judgment_id}: {e}")
                    continue  # Continue with the next judgment

    except Exception as e:
        print(f"An error occurred during database operations: {e}")

    finally:
        # Close database connection
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    process_judgments()
