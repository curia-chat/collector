import pymysql

OPENAI_API_KEY = ""
QDRANT_API_KEY = ""
QDRANT_URL = ""

# MySQL connection settings
MYSQL_CONFIG = {
    'host': '',
    'port': 3306,
    'user': '',
    'password': '',
    'database': '',
    'ssl': {
        'ca': 'ca-certificates.crt',
    }
}


def get_mysql_connection():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        print("Connection to MySQL-server successful!")
        return conn
    except pymysql.Error as err:
        print(f"Connection to MySQL-Server failed for the following reason: {err}")
        return None


# Define the table structure for the 'Judgments' table
DB_STRUCTURE = {
    'Judgments': {
        'columns': {
            'docid': 'INT(11) AUTO_INCREMENT PRIMARY KEY',
            'EURLexDoc': 'VARCHAR(255)',
            'ecli': 'VARCHAR(255)',
            'case_no': 'TEXT',
            'text_de': 'MEDIUMTEXT',
            'text_de_merits': 'MEDIUMTEXT',
            'text_summary_de': 'TEXT',
            'date_decided': 'DATE',
            'provisional': 'TINYINT(1)',
            'pdf_available': 'TINYINT(1)',
            'html_available': 'TINYINT(1)',
            'is_digitized': 'TINYINT(1)',
            'is_translated_de': 'TINYINT(1)',
            'is_vectorized': 'DATETIME',
            'caselist_url': 'TEXT',
        },
        'index': [
            'ecli',
            'date_decided',
            'FULLTEXT(case_no)',   
        ]
    }
}
