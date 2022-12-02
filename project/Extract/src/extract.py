''' MODULE FOR EXTRACTING THE DATA THAT IS NEEDED FOR THE PROJECT AND INSERTING IT INTO THE DB'''

import logging
import json
import os
import requests
import psycopg2 # pylint: disable=import-error
import pandas as pd # pylint: disable=import-error
from tqdm import tqdm # pylint: disable=import-error

from constants import LIST_MONTHS
from constants import LIST_PRODUCTS
from constants import DATASET1_COLUMNS
from constants import CCAA_POSITIONS
from constants import DATASET1_NAME
from constants import DATASET1_2018_URL
from constants import DATASET1_2019_URL
from constants import DATASET1_2020_URL
from constants import DATASET5_NAME
from constants import DATASET5_URL
from constants import ROWS_TO_PARSE_2018_AND_2019
from constants import ROWS_TO_PARSE_2020
from constants import ROWS_TO_PARSE_DEC_2019
from db_constants import BASE_DATABASE
from db_constants import AGRICULTURE_DATABASE
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


def create_df1_by_year(url_year, year):
    ''' Method for obtaining the df1 of a given year'''
    if year == 2020:
        df1 = create_df1_by_year_by_month(url_year, 'enero', year)
    else:
        df1 = create_df1_by_year_by_month(url_year, 'Enero', year)

    for month in LIST_MONTHS:
        df1 = pd.concat([df1, create_df1_by_year_by_month(url_year, month, year)])

    return df1


def create_df1_by_year_by_month(url_year, month, year):
    ''' Method for obtaining the df1 of a given year in a given month '''
    rows_to_parse = ROWS_TO_PARSE_2018_AND_2019
    if year == 2020:
        month = month.lower()
        rows_to_parse = ROWS_TO_PARSE_2020
    elif year == 2019 and month == 'Diciembre':
        rows_to_parse = ROWS_TO_PARSE_DEC_2019
    dataset1 = pd.read_excel(
        url_year,
        sheet_name = month,
        usecols = 'A:G',
        skiprows = 2,
        nrows = rows_to_parse
    )
    df1 = dataset1[dataset1['Unnamed: 0'].isin(LIST_PRODUCTS)]
    df1 = df1.rename(columns = {'Unnamed: 0' : 'Producto'})
    df1['CCAA'] = ['Total Nacional' for i in range(df1.shape[0])]


    for key, val in CCAA_POSITIONS.items():
        monthly_df = pd.read_excel(
            url_year,
            sheet_name = month,
            usecols = val,
            skiprows = 3,
            header = None,
            names = DATASET1_COLUMNS,
            nrows = rows_to_parse
        )
        monthly_df = monthly_df[monthly_df['Producto'].isin(LIST_PRODUCTS)]
        monthly_df['CCAA'] = [str(key) for i in range(monthly_df.shape[0])]
        df1 = pd.concat([df1, monthly_df])

    df1['Año'] = [year for i in range(df1.shape[0])]
    df1['Mes'] = [month.capitalize() for i in range(df1.shape[0])]
    df1 = df1.set_index(['Año', 'Mes', 'CCAA', 'Producto'])

    return df1

def download_dataset1_2018():
    ''' Method for downloading dataset 1: 2018 data only '''
    return create_df1_by_year(DATASET1_2018_URL, 2018)

def download_dataset1_2019():
    ''' Method for downloading dataset 1: 2019 data only '''
    return create_df1_by_year(DATASET1_2019_URL, 2019)

def download_dataset1_2020():
    ''' Method for downloading dataset 1: 2019 data only '''
    return create_df1_by_year(DATASET1_2020_URL, 2020)

def download_dataset1():
    ''' Method for downloading dataset 1 '''
    logging.debug('DATASET 1: START')
    dataset1_2018 = download_dataset1_2018()
    logging.debug('DATASET 2: START')
    dataset1_2019 = download_dataset1_2019()
    logging.debug('DATASET 3: START')
    dataset1_2020 = download_dataset1_2020()
    logging.debug('CONCATENATING...')
    pd.concat(
        [dataset1_2018, dataset1_2019, dataset1_2020]
    ).to_csv(DATASET1_NAME, float_format='%.2f')


def download_dataset5():
    ''' Method for downloading dataset 5 '''
    dataset5_response = requests.get(DATASET5_URL)
    dataset5 = pd.read_excel(dataset5_response.content)
    dataset5.to_csv(DATASET5_NAME, index = None, header = True)

def create_database():
    ''' Method that creates the raw schema '''

    with open('/home/app/src/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = BASE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )

    db_conn.autocommit = True
    logging.debug('Trying to create the database...')
    db_cursor = db_conn.cursor()
    db_cursor.execute(DROP_DATABASE_SQL)
    db_cursor.execute(DATABASE_SQL)
    logging.debug('Database created successfully!')
    db_conn.close()

def create_raw_schema():
    ''' Method to create the raw schema of the database '''

    with open('/home/app/src/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = AGRICULTURE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )

    db_conn.autocommit = True
    db_cursor = db_conn.cursor()
    logging.debug('Trying to remove the raw schema...')
    db_cursor.execute(DROP_RAW_SCHEMA_SQL)
    logging.debug('Raw schema removed successfully!')
    logging.debug('Trying to create the raw schema...')
    db_cursor.execute(RAW_SCHEMA_SQL)
    logging.debug('Raw schema created successfully!')
    logging.debug('Trying to create the consumption table...')
    db_cursor.execute(RAW_CONSUMPTION_TABLE_SQL)
    logging.debug('Consumption table created successfully!')
    logging.debug('Trying to create the covid table...')
    db_cursor.execute(RAW_COVID_TABLE_SQL)
    logging.debug('Covid table created successfully!')
    db_conn.close()


def populate_raw_schema():
    ''' Method to populate the raw schema '''
    populate_consumption_data()
    populate_covid_data()
    os.remove(DATASET1_PATH)
    os.remove(DATASET5_PATH)


def populate_consumption_data():
    ''' Method to populate the first table about consumption data '''

    with open('/home/app/src/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = AGRICULTURE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )

    db_conn.autocommit = True
    db_cursor = db_conn.cursor()
    logging.debug('Adding first dataset to the database...')

    with open(DATASET1_PATH, "r", encoding = "utf8") as file:
        lines = file.read().split("\n")

        for i, line in tqdm(enumerate(lines), "Adding consumption dataset", total=len(lines)):
            if i not in (0, len(lines) - 1):
                values = line.split(',')

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

    logging.debug('Data inserted successfully in the consumption database!')
    db_conn.close()


def populate_covid_data():
    ''' Method to populate the second table about covid data '''

    with open('/home/app/src/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = AGRICULTURE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )

    db_conn.autocommit = True
    db_cursor = db_conn.cursor()
    logging.debug('Adding second dataset to the database...')

    with open(DATASET5_PATH, "r", encoding = "utf8") as file:
        lines = file.read().split("\n")

        for i, line in tqdm(enumerate(lines), "Adding covid dataset", total=len(lines)):
            if i not in (0, len(lines) - 1):
                if "\"" in line:
                    values = line.split(',')

                    if len(values) == 12:
                        values = [values[0], values[1], values[2], values[3], values[4],
                                values[5], values[6] + ', ' + values[7], values[8], values[9],
                                values[10], values[11], 'NULL']
                    else:
                        values = [values[0], values[1], values[2], values[3], values[4],
                            values[5], values[6] + values[7], values[8], values[9],
                            values[10], values[11], values[12]]
                else:
                    values = line.split(',')

                date_rep = str(values[0])
                day = int(values[1])
                month = int(values[2])
                year = int(values[3])
                cases = int(values[4])
                deaths = int(values[5])
                countries_and_territories = str(values[6])
                geo_id = str(values[7])

                if values[8] != '':
                    country_territory_code = str(values[8])
                else:
                    country_territory_code = 'NULL'

                if values[9] != 'Other' and values[9] != '':
                    pop_data_2019 = float(values[9])
                else:
                    pop_data_2019 = 'NULL'

                if values[10] != '':
                    continent_exp = str(values[10])
                else:
                    continent_exp = 'NULL'

                if values[11] != '':
                    incidence = float(values[11])
                else:
                    incidence = 'NULL'

                db_cursor.execute(INSERT_COVID_SQL.format(date_rep, day, month, year,
                    cases, deaths, countries_and_territories, geo_id, country_territory_code,
                    pop_data_2019, continent_exp, incidence))

    logging.debug('Data inserted successfully in the covid database!')
    db_conn.close()


def main():
    ''' Main method '''
    download_dataset1()
    download_dataset5()
    create_database()
    create_raw_schema()
    populate_raw_schema()


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logging.debug('Cannot extract the information due to this error: %s', exception)
