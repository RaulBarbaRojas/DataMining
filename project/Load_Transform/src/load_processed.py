''' MODULE FOR LOADING THE RAW DATA INSIDE A PSQL DATABASE '''

import logging
import psycopg2
import pandas as pd
from tqdm import tqdm

from db_constants import AGRICULTURE_DATABASE
from db_constants import USERNAME
from db_constants import PASSWORD
from db_constants import HOST
from db_constants import PORT
from db_constants import DROP_PROCESSED_SCHEMA_SQL
from db_constants import PROCESSED_SCHEMA_SQL
from db_constants import DATACARD1_TABLE_SQL
from db_constants import DATACARD2_TABLE_SQL
from db_constants import DATACARD3_TABLE_SQL
from db_constants import INSERT_DATACARD1_SQL
from db_constants import INSERT_DATACARD2_SQL
from db_constants import INSERT_DATACARD3_SQL

logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)


def load_processed_data():
    ''' Loads the processed data from the data cards '''

    datacard_h1 = pd.read_csv('/home/app/data/datacard_h1.txt', index_col = 0)
    datacard_h2 = pd.read_csv('/home/app/data/datacard_h2.txt', index_col = 0)
    datacard_h3 = pd.read_csv('/home/app/data/datacard_h3.txt', index_col = 0)

    create_processed_schema()
    populate_data_card(datacard_h1, datacard_h2, datacard_h3)


def create_processed_schema():
    ''' Method to create the processed schema of the database '''

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
        logging.debug('Trying to remove the processed schema...')
        db_cursor.execute(DROP_PROCESSED_SCHEMA_SQL)
        logging.debug('Processed schema removed successfully!')
        logging.debug('Trying to create the processed schema...')
        db_cursor.execute(PROCESSED_SCHEMA_SQL)
        logging.debug('Processed schema created successfully!')
        logging.debug('Trying to create the first data card table...')
        db_cursor.execute(DATACARD1_TABLE_SQL)
        logging.debug('First data card table created successfully!')
        logging.debug('Trying to create the second data card table...')
        db_cursor.execute(DATACARD2_TABLE_SQL)
        logging.debug('Second data card table created successfully!')
        logging.debug('Trying to create the third data card table...')
        db_cursor.execute(DATACARD3_TABLE_SQL)
        logging.debug('Third data card table created successfully!')
        db_conn.close()
    except Exception as exception:
        logging.error('Cannot create the processed schema due to this error: %s', exception)


def populate_data_card(datacard_h1, datacard_h2, datacard_h3):
    ''' Method to populate the tables about data cards '''

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
        logging.debug('Adding data cards to the database...')

        for row in tqdm(datacard_h1.iterrows(), "Adding rows in data card 1", len(datacard_h1)):
            db_cursor.execute(INSERT_DATACARD1_SQL.format(row[1][0], row[1][1], row[1][2]))

        logging.debug('Added first data card to the database!')

        for row in tqdm(datacard_h2.iterrows(), "Adding rows in data card 2", len(datacard_h2)):
            db_cursor.execute(INSERT_DATACARD2_SQL.format(row[1][0], row[1][1], row[1][2],\
                row[1][3]))

        logging.debug('Added second data card to the database!')

        for row in tqdm(datacard_h3.iterrows(), "Adding rows in data card 3", len(datacard_h3)):
            db_cursor.execute(INSERT_DATACARD3_SQL.format(row[1][0], row[1][1], row[1][2],\
                row[1][3], row[1][4], row[1][5]))

        logging.debug('Added third data card to the database!')
        logging.debug('Data inserted successfully in the database!')
        db_conn.close()
    except Exception as exception:
        logging.error('Cannot populate the data cards database due to this error: %s', exception)


def main():
    ''' Main method '''
    load_processed_data()


if __name__ == '__main__':
    main()
