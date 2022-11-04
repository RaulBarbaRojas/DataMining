''' MODULE FOR LOADING THE RAW DATA INSIDE A PSQL DATABASE '''

import psycopg2

from db_constants import BASE_DATABASE
from db_constants import RAW_DATABASE
from db_constants import USERNAME
from db_constants import PASSWORD
from db_constants import HOST
from db_constants import PORT
from db_constants import DROP_RAW_SCHEMA_SQL
from db_constants import RAW_SCHEMA_SQL

def create_raw_schema():
    ''' Method that creates the raw schema '''
    try:
        db_conn = psycopg2.connect(
        database = BASE_DATABASE,
        user = USERNAME,
        password = PASSWORD,
        host = HOST,
        port = PORT
        )

        db_conn.autocommit = True
        print('Trying to create the raw schema')
        db_cursor = db_conn.cursor()
        db_cursor.execute(DROP_RAW_SCHEMA_SQL)
        db_cursor.execute(RAW_SCHEMA_SQL)
        print('Raw schema created successfully')
        db_conn.close()
    except Exception as exception:
        print(f'Cannot create the raw schema due to this error: {exception}')

def main():
    ''' Main method '''
    create_raw_schema()

if __name__ == '__main__':
    main()
