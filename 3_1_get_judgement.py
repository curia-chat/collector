import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import html2text  # Bibliothek für die Konvertierung von HTML in Markdown
import sys
from urllib.parse import urlparse, urlunparse
import re

# Funktion zur URL-Umwandlung für curia.europa.eu (bleibt unverändert)
def modify_url_curia(url):
    print("Modifying curia.europa.eu URL...", file=sys.stderr)
    modified_url = url.replace('liste.jsf', 'documents.jsf')
    print(f"Modified curia.europa.eu URL: {modified_url}", file=sys.stderr)
    return modified_url

# Funktion zum Abrufen des Inhalts mit Selenium für curia.europa.eu (bleibt unverändert)
def fetch_rendered_html_selenium(url):
    print("Setting Chrome options for Selenium...", file=sys.stderr)

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')  # Festlegen der Fenstergröße

    print("Initializing WebDriver for Selenium...", file=sys.stderr)
    try:
        driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'), options=chrome_options)
    except Exception as e:
        print(f"Fehler bei der Initialisierung des WebDrivers: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print(f"Opening URL in Selenium: {url}", file=sys.stderr)
        driver.get(url)

        # Feste Wartezeit, um sicherzustellen, dass JavaScript ausgeführt wird
        wait_time = 2  # Sekunden
        print(f"Waiting for {wait_time} seconds to allow the page to load...", file=sys.stderr)
        time.sleep(wait_time)

        print("Extracting page source with Selenium...", file=sys.stderr)
        rendered_html = driver.page_source
        print("HTML extracted successfully with Selenium.", file=sys.stderr)
        return rendered_html

    except Exception as e:
        print(f"Fehler beim Abrufen der gerenderten HTML mit Selenium: {e}", file=sys.stderr)
        # Speichern Sie den HTML-Inhalt zur weiteren Analyse
        rendered_html = driver.page_source
        with open("error_rendered_page_curia.html", "w", encoding='utf-8') as f:
            f.write(rendered_html)
        print("Speichere den gerenderten HTML-Inhalt zur Fehleranalyse als 'error_rendered_page_curia.html'.", file=sys.stderr)
        return None

    finally:
        print("Quitting WebDriver for Selenium...", file=sys.stderr)
        driver.quit()
        print("WebDriver erfolgreich beendet.", file=sys.stderr)

# Funktion zum Abrufen des HTML-Inhalts der Tabelle mit Selenium für curia.europa.eu (bleibt unverändert)
def fetch_table_html(url):
    modified_url = modify_url_curia(url)
    print("Fetching rendered HTML für curia.europa.eu...", file=sys.stderr)

    rendered_html = fetch_rendered_html_selenium(modified_url)

    if rendered_html:
        print("Parsing rendered HTML für curia.europa.eu...", file=sys.stderr)
        soup = BeautifulSoup(rendered_html, 'html.parser')
        table_element = soup.find('table', class_='detail_table_documents')
        if table_element:
            print("Table gefunden! Extrahiere HTML...", file=sys.stderr)
            table_html = table_element.prettify()
            print("HTML erfolgreich extrahiert.", file=sys.stderr)
            return table_html
        else:
            print("Table mit der Klasse 'detail_table_documents' nicht gefunden.", file=sys.stderr)
            return None
    else:
        print("Fehler beim Abrufen der gerenderten HTML für curia.europa.eu.", file=sys.stderr)
        return None

# Funktion zum Extrahieren aller Links für eine bestimmte Sprache (bleibt unverändert)
def extract_language_links(html_content, target_language):
    print("Parsing HTML mit BeautifulSoup...", file=sys.stderr)
    soup = BeautifulSoup(html_content, 'html.parser')

    print("Suche nach Zeilen, die 'Judgment' und 'ECLI' enthalten...", file=sys.stderr)
    rows = soup.find_all('tr', class_='table_document_ligne')

    language_links = []  # Liste zur Speicherung aller passenden Links

    for index, row in enumerate(rows):
        print(f"Überprüfe Zeile {index + 1}...", file=sys.stderr)
        doc_cell = row.find('td', class_='table_cell_doc')

        if doc_cell and 'Judgment' in doc_cell.text and 'ECLI' in doc_cell.text:
            print(f"Zeile {index + 1} enthält 'Judgment' und 'ECLI'. Suche nach Link-Listen...", file=sys.stderr)
            link_lists = row.find_all('ul')

            for list_index, link_list in enumerate(link_lists):
                links = link_list.find_all('a', href=True)

                for link in links:
                    language = link.text.strip()
                    if language.lower() == target_language.lower():
                        print(f"Gefundener Link für Sprache '{language}': {link['href']}", file=sys.stderr)
                        language_links.append(link['href'])

            if not any(link.text.strip().lower() == target_language.lower() for link in row.find_all('a', href=True)):
                print(f"Kein Link für Sprache '{target_language}' in Zeile {index + 1} gefunden.", file=sys.stderr)
        else:
            print(f"Zeile {index + 1} enthält nicht 'Judgment' und 'ECLI'.", file=sys.stderr)

    if language_links:
        print(f"Gefunden: {len(language_links)} Link(s) für Sprache '{target_language}'.", file=sys.stderr)
    else:
        print(f"Keine Links für Sprache '{target_language}' in 'Judgment' Zeilen gefunden.", file=sys.stderr)

    return language_links

# Funktion zum Abrufen des Inhalts von "document_content" mit requests für curia.europa.eu (bleibt unverändert)
def fetch_document_content(url):
    print(f"Fetching content von: {url}", file=sys.stderr)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/112.0.0.0 Safari/537.36',
        'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("Seite erfolgreich abgerufen. Parsing Content...", file=sys.stderr)
        soup = BeautifulSoup(response.text, 'html.parser')

        document_content = soup.find('div', id='document_content')
        if document_content:
            text_length = len(document_content.get_text(strip=True))
            print(f"'document_content' Länge: {text_length} Zeichen.", file=sys.stderr)
            return document_content, text_length
        else:
            print("Kein 'document_content' div auf der Seite gefunden.", file=sys.stderr)
            return None, 0
    else:
        print(f"Fehler beim Abrufen der Seite. Statuscode: {response.status_code}", file=sys.stderr)
        return None, 0

# Neue Funktion zum dynamischen Abrufen des richtigen Dokumentinhalts für eur-lex.europa.eu mit requests
def fetch_eurlex_document_content(url):
    print("Verarbeite eur-lex.europa.eu URL...", file=sys.stderr)

    # Führen Sie die erste Anfrage mit requests durch, um Redirects zu verfolgen
    print("Führe initialen requests.get aus, um Redirects zu verfolgen...", file=sys.stderr)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/112.0.0.0 Safari/537.36',
        'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    try:
        initial_response = requests.get(url, headers=headers, allow_redirects=True)
    except Exception as e:
        print(f"Fehler bei der initialen requests.get: {e}", file=sys.stderr)
        return None, 0

    # Loggen Sie alle Redirects
    if initial_response.history:
        print("Redirect-Historie:", file=sys.stderr)
        for resp in initial_response.history:
            print(f"{resp.status_code} -> {resp.url}", file=sys.stderr)
        print(f"Endgültige URL: {initial_response.url}", file=sys.stderr)
    else:
        print("Keine Redirects gefunden.", file=sys.stderr)

    # Überprüfen Sie, ob die endgültige URL HTTPS verwendet
    parsed_final_url = urlparse(initial_response.url)
    if parsed_final_url.scheme != 'https':
        print(f"Endgültige URL verwendet nicht HTTPS: {initial_response.url}", file=sys.stderr)
    else:
        print(f"Endgültige URL verwendet HTTPS: {initial_response.url}", file=sys.stderr)

    # Modifizieren Sie die endgültige URL, indem Sie '/HTML/' nach '/TXT/' einfügen
    path_parts = parsed_final_url.path.split('/')
    try:
        txt_index = path_parts.index('TXT')  # Suche nach 'TXT' im Pfad
        # Füge 'HTML' direkt nach 'TXT' ein
        path_parts.insert(txt_index + 1, 'HTML')
        new_path = '/'.join(path_parts)
        modified_final_url = urlunparse(parsed_final_url._replace(path=new_path))
        print(f"Modifizierte endgültige URL: {modified_final_url}", file=sys.stderr)
    except ValueError:
        print("'TXT' nicht im Pfad der endgültigen URL gefunden. Keine Modifikation vorgenommen.", file=sys.stderr)
        modified_final_url = initial_response.url

    # Abrufen des Inhalts der modifizierten URL
    print(f"Rufe Inhalt der modifizierten URL ab: {modified_final_url}", file=sys.stderr)
    try:
        content_response = requests.get(modified_final_url, headers=headers)
    except Exception as e:
        print(f"Fehler beim Abrufen der modifizierten URL: {e}", file=sys.stderr)
        return None, 0

    if content_response.status_code != 200:
        print(f"Fehler beim Abrufen der modifizierten URL. Statuscode: {content_response.status_code}", file=sys.stderr)
        return None, 0

    # Parsen des Inhalts
    print("Parsing des Inhalts der modifizierten URL...", file=sys.stderr)
    soup = BeautifulSoup(content_response.text, 'html.parser')

    # Entfernen des Elements mit id="banner"
    banner = soup.find(id="banner")
    if banner:
        print("Entferne das Element mit id='banner'...", file=sys.stderr)
        banner.decompose()
    else:
        print("Kein Element mit id='banner' gefunden.", file=sys.stderr)

    # Extrahieren der ersten dreißig Zeilen
    text = soup.get_text(separator='\n').strip()
    first_thirty_lines = '\n'.join(text.split('\n')[:30]).lower()
    print(f"Erste dreißig Zeilen: {first_thirty_lines}", file=sys.stderr)

    if 'urteil' in first_thirty_lines:
        print("'Urteil' in den ersten dreißig Zeilen gefunden.", file=sys.stderr)
        text_length = len(text)
        print(f"Länge des Textes: {text_length} Zeichen.", file=sys.stderr)
        if text_length >= 2000:
            print("Textlänge erfüllt die Mindestanforderung von 2000 Zeichen.", file=sys.stderr)
            return text, text_length
        else:
            print(f"Textlänge ist zu kurz: {text_length} Zeichen.", file=sys.stderr)
            return None, 0
    else:
        print("'Urteil' nicht in den ersten dreißig Zeilen gefunden. Suche nach <a> Tag mit id='judgment'...", file=sys.stderr)
        judgment_tag = soup.find('a', id='judgment')
        if judgment_tag:
            print("<a> Tag mit id='judgment' gefunden.", file=sys.stderr)
            # Extrahieren des gesamten Inhalts nach dem <a id='judgment'> Tag inklusive des <a> Tags selbst
            # Finden der Eltern des <a> Tags, um den relevanten Kontext zu erhalten
            judgment_parent = judgment_tag.find_parent()

            if judgment_parent:
                print("Finde Eltern-Tag des <a id='judgment'> Tags...", file=sys.stderr)
                # Extrahiere das Eltern-Tag und alle folgenden Elemente
                judgment_content = ''.join([str(judgment_parent)] + [str(element) for element in judgment_parent.find_all_next()])
                judgment_text = BeautifulSoup(judgment_content, 'html.parser').get_text(separator='\n').strip()
                judgment_text_lower = judgment_text.lower()
                print(f"Länge des gefundenen Urteils: {len(judgment_text)} Zeichen.", file=sys.stderr)
                if 'urteil' in judgment_text_lower and len(judgment_text) >= 2000:
                    print("Gefundenes Urteil erfüllt die Anforderungen.", file=sys.stderr)
                    return judgment_text, len(judgment_text)
                else:
                    print("Gefundenes Urteil erfüllt nicht die Anforderungen.", file=sys.stderr)
                    return None, 0
            else:
                print("Eltern-Tag des <a id='judgment'> Tags nicht gefunden.", file=sys.stderr)
                return None, 0
        else:
            print("<a> Tag mit id='judgment' nicht gefunden.", file=sys.stderr)
            return None, 0

# Funktion zur Konvertierung von HTML nach Markdown (bleibt unverändert)
def convert_html_to_markdown(html_content):
    print("Konvertiere HTML zu Markdown...", file=sys.stderr)
    h = html2text.HTML2Text()
    h.ignore_links = False
    markdown = h.handle(str(html_content))
    return markdown

# Hauptlogik zum Abrufen des Texts
def main():
    if len(sys.argv) != 3:
        print("Usage: python3 3_1_get_judgment.py <url> <target_language>", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    target_language = sys.argv[2]

    print("Starte das Skript...", file=sys.stderr)

    # URL analysieren
    parsed_url = urlparse(url)
    netloc = parsed_url.netloc
    scheme = parsed_url.scheme

    # Sprachcode-Mapping
    language_codes = {
        'German': 'de',
        'English': 'en',
        'French': 'fr',
        'Spanish': 'es',
        # Weitere Sprachen hinzufügen, falls erforderlich
    }

    if 'eur-lex.europa.eu' in netloc:
        print("URL erkannt: eur-lex.europa.eu", file=sys.stderr)
        # Sicherstellen, dass die URL mit HTTPS beginnt
        if parsed_url.scheme != 'https':
            print("Ändere URL-Schema zu HTTPS...", file=sys.stderr)
            parsed_url = parsed_url._replace(scheme='https')
            print("Neue URL:", file=sys.stderr)
            modified_url = urlunparse(parsed_url)
        else:
            modified_url = url

        # Sprachcode basierend auf target_language abrufen
        language_code = language_codes.get(target_language, 'en')  # Standardmäßig 'en' verwenden

        # Query-String abrufen
        query_string = parsed_url.query

        # 'lg' Parameter ersetzen oder hinzufügen
        if 'lg=' in query_string:
            new_query_string = re.sub(r'lg=[^&]*', 'lg=' + language_code, query_string)
        else:
            if query_string:
                new_query_string = query_string + '&lg=' + language_code
            else:
                new_query_string = 'lg=' + language_code

        # Neue URL zusammensetzen
        modified_url = urlunparse(parsed_url._replace(query=new_query_string))
        print(f"Geänderte URL: {modified_url}", file=sys.stderr)

        # Inhalt abrufen und verarbeiten mit requests
        document_text, text_length = fetch_eurlex_document_content(modified_url)
        if document_text and text_length >= 2000:
            markdown_content = convert_html_to_markdown(document_text)
            print("Markdown-Inhalt erfolgreich abgerufen.", file=sys.stderr)
            sys.stdout.write(markdown_content)  # Nur Markdown wird über stdout ausgegeben
            sys.stdout.flush()
            print("Skript wird beendet.", file=sys.stderr)
            sys.exit(0)
        else:
            print(f"Kein geeignetes Dokument mit mindestens 2000 Zeichen für Sprache '{target_language}' gefunden.", file=sys.stderr)
            sys.exit(1)

    elif 'curia.europa.eu' in netloc:
        print("URL erkannt: curia.europa.eu", file=sys.stderr)
        # Bestehende Logik für curia.europa.eu verwenden
        html_content = fetch_table_html(url)

        if html_content:
            print("HTML-Inhalt abgerufen. Parsing startet...", file=sys.stderr)
            links = extract_language_links(html_content, target_language)

            if links:
                print(f"Gefunden: {len(links)} Link(s) für Sprache '{target_language}'.", file=sys.stderr)
                suitable_content_found = False

                for link_index, link in enumerate(links):
                    print(f"Verarbeite Link {link_index + 1}/{len(links)}: {link}", file=sys.stderr)
                    document_html, text_length = fetch_document_content(link)

                    if document_html and text_length >= 2000:
                        print(f"Geeigneter Inhalt gefunden in Link {link_index + 1} mit {text_length} Zeichen.", file=sys.stderr)
                        markdown_content = convert_html_to_markdown(document_html)
                        print("Markdown-Inhalt erfolgreich abgerufen.", file=sys.stderr)
                        sys.stdout.write(markdown_content)  # Nur Markdown wird über stdout ausgegeben
                        sys.stdout.flush()
                        print("Skript wird beendet.", file=sys.stderr)
                        sys.exit(0)
                    else:
                        if document_html:
                            print(f"Inhalt in Link {link_index + 1} ist zu kurz ({text_length} Zeichen). Suche weiter...", file=sys.stderr)
                        else:
                            print(f"Inhalt von Link {link_index + 1} konnte nicht abgerufen werden. Suche weiter...", file=sys.stderr)

                if not suitable_content_found:
                    print(f"Kein Dokument mit mindestens 2000 Zeichen für Sprache '{target_language}' gefunden.", file=sys.stderr)
                    sys.exit(1)
            else:
                print(f"Keine Links für Sprache '{target_language}' gefunden.", file=sys.stderr)
                sys.exit(1)
        else:
            print("Fehler beim Abrufen des HTML-Inhalts für curia.europa.eu.", file=sys.stderr)
            sys.exit(1)
    else:
        print("Nicht unterstützte URL-Domain.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
