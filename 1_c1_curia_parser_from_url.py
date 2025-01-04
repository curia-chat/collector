import os
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from openai import OpenAI
from settings import OPENAI_API_KEY  # OpenAI-API-Schlüssel einfügen

# OpenAI Client konfigurieren
client = OpenAI(api_key=OPENAI_API_KEY)

# Funktion, um GPT-4o-mini zur Datumsumwandlung zu verwenden
def ask_gpt_for_date(date_str):
    prompt = f"Please convert the following date to the format YYYY-MM-DD: {date_str}"
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        if response.choices:
            answer = response.choices[0].message.content.strip()
            match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", answer)
            if match:
                found_date = match.group(0)
                print(f"Found date using GPT: {found_date}")
                return found_date
        return ""
    except Exception as e:
        print(f"Error calling GPT-4 API: {e}")
        return ""

# Funktion zur Bereinigung des Urteiltitels
def clean_judgment_title(title):
    return re.sub(r"\((C-|T-)\d+.*", "", title).strip()

# Funktion zur Extraktion der URL aus einem javascript:window.open(...) Link
def extract_url_from_javascript(js_link):
    match = re.search(r"window\.open\('([^']+)'", js_link)
    if match:
        return match.group(1)
    return js_link

# Funktion zur Konvertierung des Datumsformats in YYYY-MM-DD
def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        print(f"Date parsing failed for {date_str}, asking GPT for help.")
        return ask_gpt_for_date(date_str)

# Hauptfunktion zum Parsen des HTMLs von einer URL
def parse_html_to_csv_from_url(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    case_numbers = []
    judgment_dates = []
    judgment_titles = []
    judgment_links = []
    case_status = []
    root_cases = []

    see_case_pattern = re.compile(r"See Case (\d+/\d+)")

    rows = soup.find_all("tr")
    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 2:
            continue

        case_number = columns[0].get_text(strip=True)
        links = columns[0].find_all("a")
        href_list = [extract_url_from_javascript(link.get("href")) for link in links if link.get("href")]
        case_link = ", ".join(href_list) if href_list else "None"

        judgment_info = columns[1].text.strip()
        judgment_date = ""
        judgment_title = ""
        status = ""
        root_case = ""

        if "Judgment of" in judgment_info or "Order of" in judgment_info:
            split_info = judgment_info.split(",")
            if len(split_info) > 1:
                judgment_date = split_info[0].replace("Judgment of", "").replace("Order of", "").strip()
                judgment_date = convert_date_format(judgment_date)
                judgment_title = split_info[1].strip()
        else:
            status = "Removed from the register"

        match = see_case_pattern.search(judgment_info)
        if match:
            root_case = match.group(1)

        case_numbers.append(case_number)
        judgment_dates.append(judgment_date)
        judgment_titles.append(judgment_title)
        judgment_links.append(case_link)
        case_status.append(status)
        root_cases.append(root_case)

    # DataFrame erstellen
    df = pd.DataFrame({
        "Case Number": case_numbers,
        "Judgment Date": judgment_dates,
        "Judgment Title": judgment_titles,
        "Case Link": judgment_links,
        "Status": case_status,
        "Referenziertes Root Case": root_cases
    })

    # Erstellung der Child-Cases
    root_to_child_cases = {}
    for idx, row in df.iterrows():
        if row["Referenziertes Root Case"]:
            root_case = row["Referenziertes Root Case"]
            child_case = row["Case Number"]
            if root_case not in root_to_child_cases:
                root_to_child_cases[root_case] = []
            root_to_child_cases[root_case].append(child_case)

    child_cases_column = []
    for idx, row in df.iterrows():
        root_case = row["Case Number"]
        if root_case in root_to_child_cases:
            child_cases_column.append(", ".join(root_to_child_cases[root_case]))
        else:
            child_cases_column.append("")
    df["Child-Cases"] = child_cases_column

    # Speicherort sicherstellen
    os.makedirs("caselist_csv_c1", exist_ok=True)

    # Speichere die CSV mit aktuellem Datum
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"caselist_csv_c1/{current_date}_c1_cases.csv"
    df.to_csv(output_file, index=False)
    print(f"CSV-Datei gespeichert: {output_file}")

# URL der Webseite
url = "https://curia.europa.eu/en/content/juris/c1.htm"
parse_html_to_csv_from_url(url)
