import pandas as pd
import requests
from bs4 import BeautifulSoup
from settings import get_mysql_connection

# Funktion, um die ECLI aus dem HTML zu extrahieren
def fetch_ecli_from_url(url):
    try:
        # HTTP-Anfrage mit Unterstützung für Redirects
        response = requests.get(url, allow_redirects=True, timeout=10)
        if response.status_code != 200:
            print(f"Fehler beim Abrufen der Seite {url}. Statuscode: {response.status_code}")
            return None
        
        # Umleiten-URL ausgeben (falls Redirect erfolgt)
        if response.history:
            print(f"Redirects: {[r.url for r in response.history]}")
        print(f"Endgültige URL: {response.url}")

        # BeautifulSoup zur HTML-Verarbeitung
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


# Funktion zur Überprüfung und Ergänzung der ECLI
def check_cases_and_fetch_ecli(csv_file):
    # CSV-Datei laden
    df = pd.read_csv(csv_file)
    
    # Filtere nur Fälle mit gesetztem Datum
    df = df[df["Judgment Date"].notna()]
    
    # Neue Spalte für ECLI initialisieren
    df["ECLI"] = None

    # Verbindung zur Datenbank herstellen
    conn = get_mysql_connection()
    not_found_cases = []

    try:
        with conn.cursor() as cursor:
            for idx, row in df.iterrows():
                case_no = row["Case Number"]
                case_link = row["Case Link"]

                # Überprüfe, ob der Fall in der Datenbank existiert
                query = """
                    SELECT COUNT(*)
                    FROM Judgments
                    WHERE case_no LIKE %s
                """
                cursor.execute(query, (f"%{case_no}%",))
                result = cursor.fetchone()

                # Falls der Fall nicht gefunden wurde, füge ihn der Liste hinzu
                if result[0] == 0:
                    ecli = None
                    if case_link and case_link != "None":
                        ecli = fetch_ecli_from_url(case_link)

                    not_found_cases.append({
                        "Case Number": case_no,
                        "Judgment Date": row["Judgment Date"],
                        "Judgment Title": row["Judgment Title"],
                        "Case Link": case_link,
                        "ECLI": ecli
                    })
    except Exception as e:
        print(f"Fehler bei der Datenbankabfrage: {e}")
    finally:
        conn.close()

    # Ergebnisse als DataFrame speichern
    not_found_df = pd.DataFrame(not_found_cases)
    
    # Speicherort sicherstellen
    output_file = "not_found_cases_with_ecli.csv"
    not_found_df.to_csv(output_file, index=False)
    print(f"Nicht gefundene Fälle mit ECLI gespeichert in: {output_file}")

# Hauptprogramm
if __name__ == "__main__":
    csv_file_path = "caselist_csv_c1/2024-11-28_c1_cases.csv"  # Pfad zur CSV-Datei
    check_cases_and_fetch_ecli(csv_file_path)
