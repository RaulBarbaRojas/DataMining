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
from db_constants import INSERT_CONSUMPTION_SQL
from db_constants import INSERT_COVID_SQL
from db_constants import DATASET1_PATH
from db_constants import DATASET5_PATH

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
    populate_consumption_data()
    populate_covid_data()


def populate_consumption_data():
    ''' Method to populate the first table about consumption data '''

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
        logging.debug('Adding first dataset to the database')

        file = open(DATASET1_PATH, "r")
        lines = file.read().split("\n")

        for i in range(len(lines)):
            if i != 0 and i != len(lines)-1:
                values = lines[i].split(',')
                
                year = int(values[0])
                month = str(values[1])
                region = str(values[2])
                product = str(values[3])

                if values[4] != '':
                    consumption_per_capita = float(values[4])
                else:
                    consumption_per_capita = 'NULL'

                if values[5] != '':
                    expenses_per_capita = float(values[5])
                else:
                    expenses_per_capita = 'NULL'

                if values[6] != '':
                    market_penetration = float(values[6])
                else:
                    market_penetration = 'NULL'

                if values[7] != '':
                    average_price_per_kg_or_l = float(values[7])
                else:
                    average_price_per_kg_or_l = 'NULL'

                if values[8] != '':
                    value_in_thousands_of_euros = float(values[8])
                else:
                    value_in_thousands_of_euros = 'NULL'
                
                if values[9] != '':
                    volume_in_thousands_of_kg_or_l = float(values[9])
                else:
                    volume_in_thousands_of_kg_or_l = 'NULL'

                db_cursor.execute(INSERT_CONSUMPTION_SQL.format(year, month, region, product,
                    consumption_per_capita, expenses_per_capita, market_penetration,
                    average_price_per_kg_or_l, value_in_thousands_of_euros,
                    volume_in_thousands_of_kg_or_l))

        logging.debug('Data inserted successfully in the consumption database')
        db_conn.close()
    except Exception as exception:
        logging.error(f'Cannot populate the consumption database due to this error: {exception}')


def populate_covid_data():
    ''' Method to populate the second table about covid data '''

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
        logging.debug('Adding second dataset to the database')

        file = open(DATASET5_PATH, "r")
        lines = file.read().split("\n")

        for i in range(len(lines)):
            if i != 0 and i != len(lines)-1:
                if "\"" in lines[i]:
                    values = lines[i].split(',')

                    if len(values) == 12:
                        values = [values[0], values[1], values[2], values[3], values[4],
                                values[5], values[6] + ', ' + values[7], values[8], values[9],
                                values[10], values[11], 'NULL']
                    else:
                        values = [values[0], values[1], values[2], values[3], values[4],
                            values[5], values[6] + values[7], values[8], values[9],
                            values[10], values[11], values[12]]
                else:
                    values = lines[i].split(',')
                
                dateRep = str(values[0])
                day = int(values[1])
                month = int(values[2])
                year = int(values[3])
                cases = int(values[4])
                deaths = int(values[5])
                countriesAndTerritories = str(values[6])
                geoId = str(values[7])

                if values[8] != '':
                    countryTerritoryCode = str(values[8])
                else:
                    countryTerritoryCode = 'NULL'

                if values[9].isnumeric():
                    popData2019 = float(values[9])
                else:
                    popData2019 = 'NULL'

                if values[10] != '':
                    continentExp = str(values[10])
                else:
                    continentExp = 'NULL'

                if values[11] != '':
                    incidence = float(values[11])
                else:
                    incidence = 'NULL'

                db_cursor.execute(INSERT_COVID_SQL.format(dateRep, day, month, year,
                    cases, deaths, countriesAndTerritories, geoId, countryTerritoryCode,
                    popData2019, continentExp, incidence))

        logging.debug('Data inserted successfully in the covid database')
        db_conn.close()
    except Exception as exception:
        logging.error(f'Cannot populate the covid database due to this error: {exception}')


def main():
    ''' Main method '''
    create_database()
    create_raw_schema()
    populate_raw_schema()


if __name__ == '__main__':
    main()
