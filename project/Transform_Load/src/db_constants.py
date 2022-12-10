''' DOCKER PATH '''

DATASET1_PATH = '/home/app/data/dataset1.txt'
DATASET5_PATH = '/home/app/data/dataset5.txt'


''' DATABASE NAMES '''

BASE_DATABASE = 'postgres'
AGRICULTURE_DATABASE = 'agriculture_db'


''' DATABASE ACCESS CONSTANTS '''

HOST = 'host.docker.internal'
PORT = '5432'


''' SQL: CREATE DATABASE '''

DATABASE_SQL = 'CREATE DATABASE agriculture_db'
DROP_DATABASE_SQL = 'DROP DATABASE IF EXISTS agriculture_db'


''' SQL: CREATE RAW SCHEMA (IF NOT EXISTS) '''

RAW_SCHEMA_SQL = 'CREATE SCHEMA agriculture_raw AUTHORIZATION CURRENT_USER'
DROP_RAW_SCHEMA_SQL = 'DROP SCHEMA IF EXISTS agriculture_raw CASCADE'

PROCESSED_SCHEMA_SQL = 'CREATE SCHEMA agriculture_processed AUTHORIZATION CURRENT_USER'
DROP_PROCESSED_SCHEMA_SQL = 'DROP SCHEMA IF EXISTS agriculture_processed CASCADE'

RAW_CONSUMPTION_TABLE_SQL = ''' CREATE TABLE agriculture_raw.consumption (
                                    year INTEGER,
                                    month TEXT,
                                    CCAA TEXT,
                                    product TEXT,
                                    consumption_per_capita REAL,
                                    expenses_per_capita REAL,
                                    market_penetration REAL,
                                    average_price_per_kg_or_l REAL,
                                    value_in_thousands_of_euros REAL,
                                    volume_in_thousands_of_kg_or_l REAL,
                                    PRIMARY KEY(year, month, CCAA, product)
                                )'''

RAW_COVID_TABLE_SQL = ''' CREATE TABLE agriculture_raw.covid (
                                dateRep DATE,
                                day INTEGER,
                                month INTEGER,
                                year INTEGER,
                                cases INTEGER,
                                deaths INTEGER,
                                countriesAndTerritories TEXT,
                                geoId TEXT,
                                countryTerritoryCode TEXT,
                                popData2019 REAL,
                                continentExp TEXT,
                                incidence REAL,
                                PRIMARY KEY(dateRep, countriesAndTerritories)
                            )'''

DATACARD1_TABLE_SQL = ''' CREATE TABLE agriculture_processed.datacard1 (
                                    month INTEGER,
                                    expenses_per_capita REAL,
                                    average_price_per_kg_or_l REAL
                                )'''

DATACARD2_TABLE_SQL = ''' CREATE TABLE agriculture_processed.datacard2 (
                                    month INTEGER,
                                    expenses_per_capita REAL,
                                    average_price_per_kg_or_l REAL,
                                    cummulative_cases_for_100k REAL
                                )'''

DATACARD3_TABLE_SQL = ''' CREATE TABLE agriculture_processed.datacard3 (
                                    month INTEGER,
                                    ccaa INTEGER,
                                    product INTEGER,
                                    expenses_per_capita REAL,
                                    market_penetration REAL,
                                    average_price_per_kg_or_l REAL
                                )'''

INSERT_CONSUMPTION_SQL = ''' INSERT INTO agriculture_raw.consumption(
                                    year,
                                    month,
                                    CCAA,
                                    product,
                                    consumption_per_capita,
                                    expenses_per_capita,
                                    market_penetration,
                                    average_price_per_kg_or_l,
                                    value_in_thousands_of_euros,
                                    volume_in_thousands_of_kg_or_l) VALUES
                                    ({0}, '{1}', '{2}', '{3}', {4}, {5},
                                    {6}, {7}, {8}, {9});'''

INSERT_COVID_SQL = ''' INSERT INTO agriculture_raw.covid(
                                dateRep,
                                day,
                                month,
                                year,
                                cases,
                                deaths,
                                countriesAndTerritories,
                                geoId,
                                countryTerritoryCode,
                                popData2019,
                                continentExp,
                                incidence) VALUES
                                ('{0}', {1}, {2}, {3}, {4}, {5},
                                '{6}', '{7}', '{8}', {9}, '{10}', {11});'''

INSERT_DATACARD1_SQL = ''' INSERT INTO agriculture_processed.datacard1(
                                    month,
                                    expenses_per_capita,
                                    average_price_per_kg_or_l) VALUES
                                    ({0}, {1}, {2});'''

INSERT_DATACARD2_SQL = ''' INSERT INTO agriculture_processed.datacard2(
                                    month,
                                    expenses_per_capita,
                                    average_price_per_kg_or_l,
                                    cummulative_cases_for_100k) VALUES
                                    ({0}, {1}, {2}, {3});'''

INSERT_DATACARD3_SQL = ''' INSERT INTO agriculture_processed.datacard3(
                                    month,
                                    ccaa,
                                    product,
                                    expenses_per_capita,
                                    market_penetration,
                                    average_price_per_kg_or_l) VALUES
                                    ({0}, {1}, {2}, {3}, {4}, {5});'''


''' SQL: SELECT DATABASE '''

SELECT_DATASET1 = '''SELECT * FROM agriculture_raw.consumption'''
SELECT_DATASET5 = '''SELECT * FROM agriculture_raw.covid'''
