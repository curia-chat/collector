import pandas as pd
import requests
from bs4 import BeautifulSoup
from settings import get_mysql_connection
import re

# Funktion, um die ECLI aus dem HTML zu extrahieren
def fetch_ecli_from_url(url):
    try:
        response = requests.get(url, allow_redirects=True, timeout=10)
        if response.status_code != 200:
            print(f"Fehler beim Abrufen der Seite {url}. Statuscode: {response.status_code}")
            return None

        if response.history:
            print(f"Redirects: {[r.url for r in response.history]}")
        print(f"Endgültige URL: {response.url}")

        soup = BeautifulSoup(response.content, "html.parser")
        ecli_paragraph = soup.find("p", string=lambda s: s and "ECLI identifier: " in s)
        if ecli_paragraph:
            ecli_text = ecli_paragraph.string
            ecli = ecli_text.replace("ECLI identifier: ", "").strip()
            print(f"ECLI abgerufen: {ecli}")
            return ecli
        else:
            print(f"ECLI nicht gefunden auf Seite: {url}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"HTTP-Fehler beim Abrufen der URL {url}: {e}")
        return None
    except Exception as e:
        print(f"Allgemeiner Fehler beim Abrufen der ECLI von {url}: {e}")
        return None

# Funktion zur Erstellung des docid
def generate_docid(ecli):
    ecli_numbers = re.sub(r'\D', '', ecli)
    return f"777{ecli_numbers}"

# Funktion, um "C-" vor jedes Aktenzeichen zu setzen
def add_c_prefix(case_number):
    cases = case_number.split(", ")
    prefixed_cases = [f"C-{case.strip()}" for case in cases]
    return ", ".join(prefixed_cases)

# Funktion zum Einfügen eines neuen Eintrags in die Datenbank
def insert_new_record(cursor, docid, case_no, ecli, date_decided, caselist_url):
    # Füge Standardwert NULL hinzu, wenn kein Datum vorhanden ist
    date_decided = date_decided if date_decided and date_decided != "nan" else None

    insert_query = """
        INSERT INTO Judgments (docid, case_no, ecli, date_decided, caselist_url, datetime_added)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    cursor.execute(insert_query, (docid, case_no, ecli, date_decided, caselist_url))

# Funktion zur Verarbeitung der CSV-Datei und zum Hinzufügen neuer Einträge in die Datenbank
def process_csv_and_add_to_db(csv_file):
    # CSV laden
    df = pd.read_csv(csv_file)

    # Filter: Zeilen ohne 'Case Link' ignorieren und nur Fälle ohne 'Referenziertes Root Case'
    df = df[df['Case Link'].notna() & df['Referenziertes Root Case'].isna()]

    # Verbindung zur MySQL-Datenbank herstellen
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            case_no = row['Case Number']
            date_decided = row['Judgment Date']
            caselist_url = row['Case Link']
            child_cases = row['Child-Cases']

            # Sicherstellen, dass Judgment Date validiert wird
            if pd.isna(date_decided) or date_decided.strip() == "" or date_decided == "nan":
                print(f"Ungültiges Datum für Fall {case_no}. Überspringe...")
                continue

            # Überprüfen, ob Fall bereits in der Datenbank existiert
            search_query = "SELECT COUNT(*) FROM Judgments WHERE case_no LIKE %s"
            cursor.execute(search_query, (f"%{case_no}%",))
            result = cursor.fetchone()

            if result[0] == 0:
                # ECLI abrufen
                ecli = fetch_ecli_from_url(caselist_url)
                if not ecli:
                    print(f"ECLI konnte für {case_no} nicht abgerufen werden. Überspringe...")
                    continue

                # docid generieren
                docid = generate_docid(ecli)

                # Fallnummer mit Child-Cases erstellen
                if pd.notna(child_cases) and child_cases:
                    case_no_with_children = f"{case_no}, {child_cases}"
                else:
                    case_no_with_children = case_no

                # "C-" vor alle Aktenzeichen setzen
                case_no_with_children = add_c_prefix(case_no_with_children)

                # Sicherstellen, dass alle Felder korrekt typisiert sind
                date_decided = str(date_decided).strip()
                caselist_url = caselist_url.strip()

                # Neuen Eintrag einfügen
                print(f"Einfügen: {docid}, {case_no_with_children}, {ecli}, {date_decided}, {caselist_url}")
                insert_new_record(cursor, docid, case_no_with_children, ecli, date_decided, caselist_url)

                # Änderungen direkt in der Datenbank speichern
                conn.commit()
            else:
                print(f"Fall {case_no} bereits in der Datenbank vorhanden. Überspringe...")

    conn.close()

# Hauptprogramm
if __name__ == "__main__":
    csv_file_path = "caselist_csv_c1/2024-11-28_c1_cases.csv"  # Pfad zur CSV-Datei
    print(f"Verarbeite CSV-Datei: {csv_file_path}")
    process_csv_and_add_to_db(csv_file_path)
