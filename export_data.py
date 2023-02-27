import mysql.connector
from dotenv import load_dotenv
import os


# loading the environment variables
load_dotenv()
HOST = os.environ.get("HOST")
DATABASE = os.environ.get("DATABASE")
DBUSER = os.environ.get("DBUSER")
PASSWORD = os.environ.get("PASSWORD")


def fetch_table_data(table_name):
    cnx = mysql.connector.connect(
        host=HOST,
        database=DATABASE,
        user=DBUSER,
        password=PASSWORD
    )

    cursor = cnx.cursor()
    cursor.execute('SELECT * FROM ' + table_name)

    header = [row[0] for row in cursor.description]
    rows = cursor.fetchall()

    # close connectiom
    cnx.close()
    return header, rows

def export(table_name):
    header, rows = fetch_table_data(table_name)

    # create CSV file
    with open(table_name + '.csv', 'w') as f:
        # write header
        f.write(','.join(header) + '\n')

        for row in rows:
            f.write(','.join(str(r) for r in row) + '\n')


    print(f'{len(rows)} rows written successfully to {f.name}')


# Tables to exported
export('Employee')
export('Department')