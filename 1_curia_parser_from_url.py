import os
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from openai import OpenAI  # OpenAI GPT API import
from settings import OPENAI_API_KEY  # Importiere den OpenAI API Key

# OpenAI Client konfigurieren
client = OpenAI(api_key=OPENAI_API_KEY)

# Funktion, um GPT-4o-mini zur Datumsumwandlung zu verwenden
def ask_gpt_for_date(date_str):
    prompt = f"Please convert the following date to the format YYYY-MM-DD: {date_str}"
    
    try:
        # OpenAI API-Aufruf mit GPT-4o-mini
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # GPT-4o-mini verwenden
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extrahiere die Antwort aus der API
        if response.choices:
            answer = response.choices[0].message.content.strip()
            # Suche nach einem Datum im Format YYYY-MM-DD
            match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", answer)
            if match:
                found_date = match.group(0)
                print(f"Found date: {found_date}")  # Ausgabe des gefundenen Datums
                return found_date  # Rückgabe des gefundenen Datums
        return ""  # Rückgabe eines leeren Wertes, wenn kein Datum gefunden wurde
    except Exception as e:
        print(f"Error calling GPT-4 API: {e}")
        return ""

# Funktion zur Bereinigung des Urteiltitels
def clean_judgment_title(title):
    return re.sub(r"\((C-|T-)\d+.*", "", title).strip()

# Funktion zur Umwandlung des Datumsformats in YYYY-MM-DD
def convert_date_format(date_str):
    try:
        date_str = str(date_str)
        return datetime.strptime(date_str, "%d %B %Y").strftime("%Y-%m-%d")
    except ValueError:
        print(f"Date parsing failed for {date_str}, asking GPT-4 for help.")
        return ask_gpt_for_date(date_str)

# Funktion zum Extrahieren der URL aus einem javascript:window.open(...) Link
def extract_url_from_javascript(js_link):
    match = re.search(r"window\.open\('([^']+)'", js_link)
    if match:
        return match.group(1)
    return js_link

# Hauptfunktion zum Laden von HTML von einer URL und Erstellen der CSV-Datei
def parse_html_to_csv_from_url(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    case_numbers = []
    judgment_dates = []
    judgment_titles = []
    ecli_ids = []
    judgment_links = []
    child_case_root = []

    see_case_pattern = re.compile(r"see Case (C-|T-)\d+/\d+(?: [A-Z])?")

    rows = soup.find_all("tr")

    for row in rows:
        columns = row.find_all("td")

        if len(columns) < 2:
            continue

        case_number = columns[0].get_text(strip=True)

        links = columns[0].find_all("a")
        href_list = []
        for link in links:
            href = link.get("href")
            if href:
                clean_href = extract_url_from_javascript(href)
                href_list.append(clean_href)

        case_link = ", ".join(href_list) if href_list else "None"

        if "Removed from the register" in columns[1].text:
            continue

        judgment_info = columns[1].text.strip()
        judgment_date = ""
        judgment_title = ""
        ecli = ""

        if "ECLI:" in judgment_info:
            split_info = judgment_info.split("ECLI:")
            judgment_title = split_info[0].split(",")[1].strip() if "," in split_info[0] else split_info[0].strip()
            ecli = "ECLI:" + split_info[1].strip()

            if "Judgment of" in split_info[0]:
                judgment_date = split_info[0].split(",")[0].replace("Judgment of", "").strip()
                judgment_date = convert_date_format(judgment_date)

        cleaned_judgment_title = clean_judgment_title(judgment_title)

        match = see_case_pattern.search(judgment_info)
        if match:
            root_case = match.group().split("see Case ")[1]
            child_case_root.append(root_case)
        else:
            ref_links = columns[1].find_all("a")
            if ref_links:
                href_text = ref_links[-1].get_text(strip=True)
                if see_case_pattern.match("see Case " + href_text):
                    child_case_root.append(href_text)
                else:
                    child_case_root.append("")
            else:
                child_case_root.append("")

        case_numbers.append(case_number)
        judgment_dates.append(judgment_date)
        judgment_titles.append(cleaned_judgment_title)
        ecli_ids.append(ecli)
        judgment_links.append(case_link)

    # Erstelle ein DataFrame mit bereinigten Daten
    df = pd.DataFrame({
        "Aktenzeichen": case_numbers,
        "Datum des Urteils": judgment_dates,
        "Bezeichnung des Urteils": judgment_titles,
        "ECLI": ecli_ids,
        "Website des Urteils": judgment_links,
        "Referenziertes Root Case": child_case_root
    })

    # Erstelle eine Spalte für Child-Cases durch Mapping von Wurzelfällen zu ihren Unterfällen
    root_to_child_cases = {}

    # Erster Durchgang: Erstelle ein Dictionary, wo Wurzelfälle auf ihre Unterfälle abgebildet werden
    for idx, row in df.iterrows():
        if row["Referenziertes Root Case"]:
            root_case = row["Referenziertes Root Case"]
            child_case = row["Aktenzeichen"]
            if root_case not in root_to_child_cases:
                root_to_child_cases[root_case] = []
            root_to_child_cases[root_case].append(child_case)

    # Zweiter Durchgang: Liste alle Unterfälle für den jeweiligen Wurzelfall auf
    child_cases_column = []
    for idx, row in df.iterrows():
        root_case = row["Aktenzeichen"]
        if root_case in root_to_child_cases:
            child_cases_column.append(", ".join(root_to_child_cases[root_case]))
        else:
            child_cases_column.append("")

    # Füge die Unterfälle als neue Spalte hinzu
    df["Child-Cases"] = child_cases_column

    # Stelle sicher, dass der Ordner "caselist_csv" existiert
    os.makedirs("caselist_csv", exist_ok=True)

    # Generiere den Dateinamen mit dem aktuellen Datum
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_csv_file = f"caselist_csv/{current_date}_ecj-caselist.csv"

    # Speichere die Daten in eine CSV-Datei (überschreiben, falls bereits vorhanden)
    df.to_csv(output_csv_file, index=False)
    print(f"CSV file saved to: {output_csv_file}")

url = "https://curia.europa.eu/en/content/juris/c2_juris.htm"
parse_html_to_csv_from_url(url)
