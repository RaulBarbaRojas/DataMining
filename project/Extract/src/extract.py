''' MODULE FOR EXTRACTING THE DATA THAT IS NEEDED FOR THE PROJECT AND INSERTING IT INTO THE DB'''

import logging
import json
import requests
import psycopg2 # pylint: disable=import-error
import pandas as pd # pylint: disable=import-error
from tqdm import tqdm # pylint: disable=import-error

from constants import LIST_MONTHS
from constants import LIST_PRODUCTS
from constants import DATASET1_COLUMNS
from constants import CCAA_POSITIONS
from constants import DATASET1_2018_URL
from constants import DATASET1_2019_URL
from constants import DATASET1_2020_URL
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

    dataset1 = pd.concat(
        [dataset1_2018, dataset1_2019, dataset1_2020]
    )

    return dataset1


def download_dataset5():
    ''' Method for downloading dataset 5 '''
    dataset5_response = requests.get(DATASET5_URL)
    dataset5 = pd.read_excel(dataset5_response.content)

    return dataset5


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


def populate_raw_schema(dataset1, dataset5):
    ''' Method to populate the raw schema '''
    populate_consumption_data(dataset1)
    populate_covid_data(dataset5)


def populate_consumption_data(dataset1):
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

    for row in tqdm(dataset1.iterrows(), "Adding consumption dataset", total=len(dataset1)):
        year = int(row[0][0])
        month = str(row[0][1])
        region = str(row[0][2])
        product = str(row[0][3])

        if str(row[1][0]).lower() != 'nan' and str(row[1][0]).lower() != '':
            consumption_per_capita = float(row[1][0])
        else:
            consumption_per_capita = 'NULL'

        if str(row[1][1]).lower() != 'nan' and str(row[1][1]).lower() != '':
            expenses_per_capita = float(row[1][1])
        else:
            expenses_per_capita = 'NULL'

        if str(row[1][2]).lower() != 'nan' and str(row[1][2]).lower() != '':
            market_penetration = float(row[1][2])
        else:
            market_penetration = 'NULL'

        if str(row[1][3]).lower() != 'nan' and str(row[1][3]).lower() != '':
            average_price_per_kg_or_l = float(row[1][3])
        else:
            average_price_per_kg_or_l = 'NULL'

        if str(row[1][4]).lower() != 'nan' and str(row[1][4]).lower() != '':
            value_in_thousands_of_euros = float(row[1][4])
        else:
            value_in_thousands_of_euros = 'NULL'

        if str(row[1][5]).lower() != 'nan' and str(row[1][5]).lower() != '':
            volume_in_thousands_of_kg_or_l = float(row[1][5])
        else:
            volume_in_thousands_of_kg_or_l = 'NULL'

        db_cursor.execute(INSERT_CONSUMPTION_SQL.format(year, month, region, product,
            consumption_per_capita, expenses_per_capita, market_penetration,
            average_price_per_kg_or_l, value_in_thousands_of_euros,
            volume_in_thousands_of_kg_or_l))

    logging.debug('Data inserted successfully in the consumption database!')
    db_conn.close()


def populate_covid_data(dataset5):
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

    for row in tqdm(dataset5.iterrows(), "Adding consumption dataset", total=len(dataset5)):
        date_rep = str(row[1][0])
        day = int(row[1][1])
        month = int(row[1][2])
        year = int(row[1][3])
        cases = int(row[1][4])
        deaths = int(row[1][5])
        countries_and_territories = str(row[1][6])
        geo_id = str(row[1][7])

        if str(row[1][8]).lower() != 'nan' and str(row[1][8]).lower() != '':
            country_territory_code = str(row[1][8])
        else:
            country_territory_code = 'NULL'

        if str(row[1][9]).lower() != 'nan' and str(row[1][9]).lower() != 'other' \
            and str(row[1][9]).lower() != '':
            pop_data_2019 = float(row[1][9])
        else:
            pop_data_2019 = 'NULL'

        if str(row[1][10]).lower() != 'nan' and str(row[1][10]).lower() != '':
            continent_exp = str(row[1][10])
        else:
            continent_exp = 'NULL'

        if str(row[1][11]).lower() != 'nan' and str(row[1][11]).lower() != '':
            incidence = float(row[1][11])
        else:
            incidence = 'NULL'

        db_cursor.execute(INSERT_COVID_SQL.format(date_rep, day, month, year,
            cases, deaths, countries_and_territories, geo_id, country_territory_code,
            pop_data_2019, continent_exp, incidence))

    logging.debug('Data inserted successfully in the covid database!')
    db_conn.close()


def main():
    ''' Main method '''
    dataset1 = download_dataset1()
    dataset5 = download_dataset5()
    create_database()
    create_raw_schema()
    populate_raw_schema(dataset1, dataset5)


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logging.debug('Cannot extract the information due to this error: %s', exception)
