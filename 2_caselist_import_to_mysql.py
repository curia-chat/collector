import os
import pandas as pd
from settings import get_mysql_connection
import re
from datetime import datetime

def get_latest_csv(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
    if not files:
        print("No CSV files found in the directory.")
        return None
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return files[0]

# Function to generate docid based on ECLI
def generate_docid(ecli):
    ecli_numbers = re.sub(r'\D', '', ecli)
    return f"777{ecli_numbers}"

# Insert a new record into the MySQL database, including the datetime_added
def insert_new_record(cursor, docid, case_no, ecli, date_decided, caselist_url):
    insert_query = """
        INSERT INTO Judgments (docid, case_no, ecli, date_decided, caselist_url, datetime_added)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    cursor.execute(insert_query, (docid, case_no, ecli, date_decided, caselist_url))

# Update an existing record in the MySQL database if necessary and log the changes
def update_existing_record(cursor, db_case_no, db_ecli, db_date_decided, db_caselist_url, case_no_with_children, ecli, date_decided, caselist_url):
    updates = []
    values = []
    changes_log = []

    # Check for changes in case_no
    if db_case_no != case_no_with_children:
        updates.append("case_no = %s")
        values.append(case_no_with_children)
        changes_log.append(f"case_no: '{db_case_no}' -> '{case_no_with_children}'")

    # Check for changes in ecli
    if not db_ecli and ecli:
        updates.append("ecli = %s")
        values.append(ecli)
        changes_log.append(f"ecli: '{db_ecli}' -> '{ecli}'")

    # Check for changes in date_decided
    if not db_date_decided and date_decided:
        updates.append("date_decided = %s")
        values.append(date_decided)
        changes_log.append(f"date_decided: '{db_date_decided}' -> '{date_decided}'")

    # Check for changes in caselist_url
    if not db_caselist_url and caselist_url:
        updates.append("caselist_url = %s")
        values.append(caselist_url)
        changes_log.append(f"caselist_url: '{db_caselist_url}' -> '{caselist_url}'")

    # If there are updates to make, execute the query and log changes
    if updates:
        update_query = "UPDATE Judgments SET " + ", ".join(updates) + " WHERE case_no = %s"
        values.append(db_case_no)  # Append the original case_no to use in the WHERE clause
        cursor.execute(update_query, tuple(values))
        print(f"Updated record for case_no: {db_case_no}")
        for change in changes_log:
            print(f" - {change}")

def process_csv_and_import_to_mysql(csv_file):
    # Load the CSV file
    df = pd.read_csv(csv_file)

    # Filter out rows where "Datum des Urteils" or "Website des Urteils" is empty
    df = df[df['Datum des Urteils'].notna() & df['Website des Urteils'].notna()]

    # Connect to the MySQL database
    conn = get_mysql_connection()

    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            case_no = row['Aktenzeichen']
            ecli = row['ECLI']
            date_decided = row['Datum des Urteils']
            caselist_url = row['Website des Urteils']
            child_cases = row['Child-Cases']

            # Generate the docid based on ECLI
            docid = generate_docid(ecli)

            # Search for case_no in the database using LIKE %...%
            search_query = "SELECT case_no, ecli, date_decided, caselist_url FROM Judgments WHERE case_no LIKE %s"
            cursor.execute(search_query, (case_no + '%',))
            result = cursor.fetchone()

            # If case_no is not found, insert a new record
            if result is None:
                if pd.notna(child_cases) and child_cases:
                    case_no_with_children = f"{case_no}, {child_cases}"
                else:
                    case_no_with_children = case_no
                print(f"Inserting new record: {docid}, {case_no_with_children}, {ecli}, {date_decided}, {caselist_url}")
                insert_new_record(cursor, docid, case_no_with_children, ecli, date_decided, caselist_url)
            else:
                # Existing record found, perform updates if necessary
                db_case_no, db_ecli, db_date_decided, db_caselist_url = result
                # Create a virtual case_no with child cases for comparison
                if pd.notna(child_cases) and child_cases:
                    case_no_with_children = f"{case_no}, {child_cases}"
                else:
                    case_no_with_children = case_no

                update_existing_record(cursor, db_case_no, db_ecli, db_date_decided, db_caselist_url, case_no_with_children, ecli, date_decided, caselist_url)

        # Commit the changes
        conn.commit()

    conn.close()

def main():
    directory = "caselist_csv"
    latest_csv = get_latest_csv(directory)
    if latest_csv:
        print(f"Processing latest CSV: {latest_csv}")
        process_csv_and_import_to_mysql(latest_csv)
    else:
        print("No valid CSV file to process.")

if __name__ == "__main__":
    main()
