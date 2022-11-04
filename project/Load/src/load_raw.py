''' MODULE FOR LOADING THE RAW DATA INSIDE A PSQL DATABASE '''

import logging
import psycopg2

from db_constants import BASE_DATABASE
from db_constants import AGRICULTURE_DATABASE
from db_constants import USERNAME
from db_constants import PASSWORD
from db_constants import HOST
from db_constants import PORT
from db_constants import DATABASE_SQL
from db_constants import DROP_DATABASE_SQL
from db_constants import DROP_RAW_SCHEMA_SQL
from db_constants import RAW_SCHEMA_SQL
from db_constants import RAW_CONSUMPTION_TABLE_SQL
from db_constants import RAW_COVID_TABLE_SQL

logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)

def create_database():
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
        logging.debug('Trying to create the database')
        db_cursor = db_conn.cursor()
        db_cursor.execute(DROP_DATABASE_SQL)
        db_cursor.execute(DATABASE_SQL)
        logging.debug('Database created successfully')
        db_conn.close()
    except Exception as exception:
        logging.error(f'Cannot create the raw schema due to this error: {exception}')

def create_raw_schema():
    ''' Method to create the raw schema of the database '''
    try:
        db_conn = psycopg2.connect(
        database = AGRICULTURE_DATABASE,
        user = USERNAME,
        password = PASSWORD,
        host = HOST,
        port = PORT
        )

        db_conn.autocommit = True
        db_cursor = db_conn.cursor()
        logging.debug('Trying to remove the raw schema')
        db_cursor.execute(DROP_RAW_SCHEMA_SQL)
        logging.debug('Raw schema removed successfully')
        logging.debug('Trying to create the raw schema')
        db_cursor.execute(RAW_SCHEMA_SQL)
        logging.debug('Raw schema created successfully')
        logging.debug('Trying to create the consumption table')
        db_cursor.execute(RAW_CONSUMPTION_TABLE_SQL)
        logging.debug('Consumption table created successfully')
        logging.debug('Trying to create the covid table')
        db_cursor.execute(RAW_COVID_TABLE_SQL)
        logging.debug('Covid table created successfully')
        db_conn.close()
    except Exception as exception:
        logging.error(f'Cannot create the raw schema due to this error: {exception}')


def populate_raw_schema():
    ''' Method to populate the raw schema '''

def main():
    ''' Main method '''
    create_database()
    create_raw_schema()
    populate_raw_schema()

if __name__ == '__main__':
    main()
