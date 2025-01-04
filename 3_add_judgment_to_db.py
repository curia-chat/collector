import os
import subprocess
from settings import get_mysql_connection
import sys

# Sicherstellen, dass der Ordner 'judgment_files' existiert
os.makedirs('judgment_files', exist_ok=True)

# Funktion zur Abfrage der Urteile mit leerem 'text_de' und gefülltem 'caselist_url', aber ohne no_valid_text_de = true
def fetch_judgments_without_text(cursor, limit=200):
    query = """
        SELECT id, caselist_url
        FROM Judgments
        WHERE text_de IS NULL
          AND caselist_url IS NOT NULL
          AND (no_valid_text_de IS NULL OR no_valid_text_de = FALSE)
        LIMIT %s
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Funktion zum Aktualisieren des Urteilstextes in der Datenbank
def update_judgment_text(judgment_id, judgment_text):
    try:
        # Jede Aufgabe erhält ihre eigene Verbindung
        conn = get_mysql_connection()
        with conn.cursor() as cursor:
            update_query = """
                UPDATE Judgments
                SET text_de = %s
                WHERE id = %s
            """
            print(f"Versuche, Urteil mit ID {judgment_id} zu aktualisieren. Textlänge: {len(judgment_text)} Zeichen.")

            # Ausgabe der ersten drei Zeilen des Textes
            lines = judgment_text.split('\n')
            print("Erste drei Zeilen des Textes:")
            for i, line in enumerate(lines[:3], start=1):
                print(f"Zeile {i}: {line}")

            cursor.execute(update_query, (judgment_text, judgment_id))
            print(f"Rows affected: {cursor.rowcount}")

            # Commit der Änderung
            conn.commit()
            print("Änderung erfolgreich committet.\n")

            # Lesen Sie den aktualisierten Text und geben Sie die ersten drei Zeilen aus
            select_query = """
                SELECT text_de
                FROM Judgments
                WHERE id = %s
            """
            cursor.execute(select_query, (judgment_id,))
            result = cursor.fetchone()
            if result and result[0]:
                updated_text = result[0]
                updated_lines = updated_text.split('\n')
                print("Erste drei Zeilen des gespeicherten Textes:")
                for i, line in enumerate(updated_lines[:3], start=1):
                    print(f"Zeile {i}: {line}")
            else:
                print("Der gespeicherte Text ist leer oder nicht gefunden.")

            print(f"Urteil mit ID {judgment_id} erfolgreich aktualisiert.\n")
    except Exception as e:
        print(f"Fehler beim Aktualisieren des Urteils mit ID {judgment_id}: {e}")
    finally:
        conn.close()

# Funktion zum Markieren eines Urteils als ohne gültigen Text
def mark_as_no_valid_text(judgment_id):
    try:
        conn = get_mysql_connection()
        with conn.cursor() as cursor:
            update_query = """
                UPDATE Judgments
                SET no_valid_text_de = TRUE
                WHERE id = %s
            """
            cursor.execute(update_query, (judgment_id,))
            conn.commit()
            print(f"Urteil mit ID {judgment_id} als 'no_valid_text_de = TRUE' markiert.\n")
    except Exception as e:
        print(f"Fehler beim Markieren von Urteil mit ID {judgment_id}: {e}")
    finally:
        conn.close()

# Funktion zum Abrufen des Urteiltexts mit 3_1_get_judgement.py
def get_judgment_text(url, judgment_id):
    target_language = "German"
    try:
        # Verwende subprocess.run für einfachere Handhabung
        result = subprocess.run(
            ["python3", "3_1_get_judgement.py", url, target_language],
            stdout=subprocess.PIPE,  # Für die Markdown-Ausgabe
            stderr=subprocess.PIPE,  # Für die Debug-Ausgaben
            text=True                # Textmodus aktiviert
        )

        # Gib stderr direkt in die Konsole aus
        print(result.stderr, end="")

        # Überprüfe den Rückgabewert des Prozesses
        if result.returncode != 0:
            print(f"Fehler beim Abrufen des Texts von {url}")
            return None

        # Gib den Markdown-Inhalt zurück (stdout)
        markdown_content = result.stdout.strip()

        # Überprüfen, ob der Text mindestens 2000 Zeichen umfasst
        if len(markdown_content) >= 2000:
            print(f"Text von {url} erfolgreich abgerufen. Länge: {len(markdown_content)} Zeichen.")

            # Falls du den Text nicht in eine Datei schreiben möchtest, kommentiere diesen Teil aus
            # filename = os.path.join('judgment_files', f'judgment_{judgment_id}.md')
            # with open(filename, 'w', encoding='utf-8') as f:
            #     f.write(markdown_content)
            # print(f"Text wurde in {filename} gespeichert.\n")

            return markdown_content
        else:
            print(f"Der abgerufene Text von {url} ist zu kurz (weniger als 2000 Zeichen).")
            return None

    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")
        return None

def process_judgment(judgment):
    judgment_id, caselist_url = judgment
    print(f"Verarbeite Urteil mit ID {judgment_id} und URL {caselist_url}...")
    judgment_text = get_judgment_text(caselist_url, judgment_id)

    if judgment_text:
        update_judgment_text(judgment_id, judgment_text)
    else:
        print(f"Kein gültiger Text für Urteil mit ID {judgment_id} abgerufen. Keine Aktualisierung in der Datenbank.\n")
        # Update der Spalte no_valid_text_de in der Datenbank
        mark_as_no_valid_text(judgment_id)

def main():
    # Verbindung zur Datenbank herstellen
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            # Aktuell verwendete Datenbank ausgeben
            cursor.execute("SELECT DATABASE();")
            result = cursor.fetchone()
            if result:
                current_db = result[0]
                print(f"Aktuell verwendete Datenbank: {current_db}\n")
            else:
                print("Keine Datenbank ausgewählt.\n")

            # Urteile ohne Text abrufen
            judgments = fetch_judgments_without_text(cursor, limit=1000000)

            if not judgments:
                print("Keine Urteile zum Aktualisieren gefunden.")
                return

    except Exception as e:
        print(f"Fehler beim Abrufen der Urteile: {e}")
        return
    finally:
        conn.close()

    # Verarbeite die Urteile sequentiell
    for judgment in judgments:
        try:
            process_judgment(judgment)
        except Exception as e:
            print(f"Ein Fehler ist aufgetreten: {e}")

    print("Alle Urteile erfolgreich verarbeitet.")

if __name__ == "__main__":
    main()
