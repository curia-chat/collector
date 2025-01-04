import pymysql
from settings import get_mysql_connection

def apply_changes_to_duplicates():
    try:
        # Verbindung zur Datenbank herstellen
        conn = get_mysql_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Finden aller Duplikate und Gruppieren nach ECLI
            query = """
                SELECT id, docid, ecli, text_summary_de, EURLexDoc, caselist_url, case_no, text_de
                FROM Judgments
                WHERE ecli IN (
                    SELECT ecli
                    FROM Judgments
                    GROUP BY ecli
                    HAVING COUNT(*) > 1
                )
                ORDER BY ecli, docid;
            """
            cursor.execute(query)
            duplicates = cursor.fetchall()

            if not duplicates:
                print("Keine Duplikate gefunden.")
                return

            grouped_duplicates = {}
            for row in duplicates:
                grouped_duplicates.setdefault(row['ecli'], []).append(row)

            total_deletions = 0
            total_updates = 0

            print("Änderungen werden in der Datenbank angewendet:\n")

            for ecli, group in grouped_duplicates.items():
                # Sortiere innerhalb der Gruppe nach docid (aufsteigend)
                group.sort(key=lambda x: x['docid'])

                # Behalte den Eintrag mit der niedrigsten docid
                base = group[0]
                updates = []

                for dup in group[1:]:
                    # Übernehme Daten aus den Duplikaten in die Basiseinträge
                    if not base['text_summary_de'] and dup['text_summary_de']:
                        base['text_summary_de'] = dup['text_summary_de']
                        cursor.execute(
                            "UPDATE Judgments SET text_summary_de = %s WHERE id = %s",
                            (dup['text_summary_de'], base['id'])
                        )
                        total_updates += 1

                    if not base['EURLexDoc'] and dup['EURLexDoc']:
                        base['EURLexDoc'] = dup['EURLexDoc']
                        cursor.execute(
                            "UPDATE Judgments SET EURLexDoc = %s WHERE id = %s",
                            (dup['EURLexDoc'], base['id'])
                        )
                        total_updates += 1

                    if not base['caselist_url'] and dup['caselist_url']:
                        base['caselist_url'] = dup['caselist_url']
                        cursor.execute(
                            "UPDATE Judgments SET caselist_url = %s WHERE id = %s",
                            (dup['caselist_url'], base['id'])
                        )
                        total_updates += 1

                    if base['case_no'] != dup['case_no'] and dup['caselist_url']:
                        base['case_no'] = dup['case_no']
                        cursor.execute(
                            "UPDATE Judgments SET case_no = %s WHERE id = %s",
                            (dup['case_no'], base['id'])
                        )
                        total_updates += 1

                    if not base['text_de'] and dup['text_de']:
                        base['text_de'] = dup['text_de']
                        cursor.execute(
                            "UPDATE Judgments SET text_de = %s WHERE id = %s",
                            (dup['text_de'], base['id'])
                        )
                        total_updates += 1

                    # Markiere das Duplikat für die Löschung
                    updates.append(dup)

                # Lösche die übrigen Duplikate
                for dup in updates:
                    cursor.execute("DELETE FROM Judgments WHERE id = %s", (dup['id'],))
                    total_deletions += 1

            # Änderungen in der Datenbank bestätigen
            conn.commit()

            print(f"Gesamtzahl der Updates: {total_updates}")
            print(f"Gesamtzahl der gelöschten Einträge: {total_deletions}")

    except Exception as e:
        print(f"Fehler bei der Verarbeitung der Duplikate: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    apply_changes_to_duplicates()
