''' MODULE FOR TRANSFORMING THE DATA INSIDE A PSQL DATABASE '''

import os
import logging
import psycopg2
import pandas as pd
import sklearn.preprocessing
from bigml.api import BigML

from db_constants import AGRICULTURE_DATABASE
from db_constants import USERNAME
from db_constants import PASSWORD
from db_constants import HOST
from db_constants import PORT
from db_constants import SELECT_DATASET1
from db_constants import SELECT_DATASET5


logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)


def transform_dataset1(initial_consumption_dataset):
    ''' Performs the transformation of the dataset 1 '''

    # Dataset preprocessing
    initial_consumption_dataset.drop(columns=['consumption_per_capita', \
        'value_in_thousands_of_euros', 'volume_in_thousands_of_kg_or_l'], inplace = True)

    initial_consumption_dataset = initial_consumption_dataset\
        [initial_consumption_dataset['ccaa'] != "Total Nacional"]

    rows_with_values_to_delete = ["TOTAL PATATAS", "T.HORTALIZAS FRESCAS", \
        "OTR.HORTALIZAS/VERD.", "VERD./HORT. IV GAMA", "T.FRUTAS FRESCAS", \
        "VERDURAS DE HOJA", "FRUTAS IV GAMA"]
    initial_consumption_dataset = initial_consumption_dataset\
        [~initial_consumption_dataset['product'].isin(rows_with_values_to_delete)]

    # Null values treatment
    initial_consumption_dataset["expenses_per_capita"] = \
        initial_consumption_dataset["expenses_per_capita"].fillna(0)
    initial_consumption_dataset["average_price_per_kg_or_l"] = \
        initial_consumption_dataset["average_price_per_kg_or_l"].fillna(0)

    # Feature type checking
    initial_consumption_dataset = initial_consumption_dataset.replace("Enero", 1)
    initial_consumption_dataset = initial_consumption_dataset.replace("Febrero", 2)
    initial_consumption_dataset = initial_consumption_dataset.replace("Marzo", 3)
    initial_consumption_dataset = initial_consumption_dataset.replace("Abril", 4)
    initial_consumption_dataset = initial_consumption_dataset.replace("Mayo", 5)
    initial_consumption_dataset = initial_consumption_dataset.replace("Junio", 6)
    initial_consumption_dataset = initial_consumption_dataset.replace("Julio", 7)
    initial_consumption_dataset = initial_consumption_dataset.replace("Agosto", 8)
    initial_consumption_dataset = initial_consumption_dataset.replace("Septiembre", 9)
    initial_consumption_dataset = initial_consumption_dataset.replace("Octubre", 10)
    initial_consumption_dataset = initial_consumption_dataset.replace("Noviembre", 11)
    initial_consumption_dataset = initial_consumption_dataset.replace("Diciembre", 12)

    initial_consumption_dataset['month'] = initial_consumption_dataset['month'].astype('int64')

    # Outliers removal
    consumption_dataset_no_outliers = initial_consumption_dataset.drop(
        initial_consumption_dataset.index[initial_consumption_dataset['expenses_per_capita'] > \
            initial_consumption_dataset['expenses_per_capita'].mean() + \
            2 * initial_consumption_dataset['expenses_per_capita'].std()]
    )

    consumption_dataset_no_outliers = initial_consumption_dataset.drop(
        initial_consumption_dataset.index[initial_consumption_dataset['market_penetration'] > \
            initial_consumption_dataset['market_penetration'].mean() + \
            2 * initial_consumption_dataset['market_penetration'].std()]
    )

    consumption_dataset_no_outliers = initial_consumption_dataset.drop(
        initial_consumption_dataset.index[initial_consumption_dataset['average_price_per_kg_or_l'] \
            > initial_consumption_dataset['average_price_per_kg_or_l'].mean() + \
            2 * initial_consumption_dataset['average_price_per_kg_or_l'].std()]
    )

    remove_consumption_anomalies(consumption_dataset_no_outliers)

    return consumption_dataset_no_outliers


def remove_consumption_anomalies(consumption_dataset_no_outliers):
    ''' Removes the anomalies in the consumption dataset using bigML '''

    api = BigML('sma_team_bgks', '4a3eda982d8771f3b55de8d617ad4c8334cfb401')

    consumption_dataset_no_outliers.to_csv('data/temp_consumption.txt')

    logging.debug('Connecting to bigML...')
    source = api.create_source('data/temp_consumption.txt')
    api.ok(source)

    logging.debug('Adding consumption dataset to bigML...')
    dataset = api.create_dataset(source)
    api.ok(dataset)

    logging.debug('Creating anomaly detector for consumption dataset...')
    anomaly = api.create_anomaly(dataset)
    api.ok(anomaly)

    logging.debug('Detecting anomalies in consumption dataset...')
    test_dataset = dataset
    batch_anomaly_score = api.create_batch_anomaly_score(anomaly, test_dataset)
    api.ok(batch_anomaly_score)
    api.download_batch_anomaly_score(batch_anomaly_score, filename='data/temp_consumption.txt')

    anomaly_scores = pd.read_csv('data/temp_consumption.txt')

    consumption_dataset_no_outliers.reset_index(inplace = True)
    consumption_dataset_no_outliers = consumption_dataset_no_outliers.join(anomaly_scores)

    consumption_dataset_no_outliers = consumption_dataset_no_outliers\
        [~(consumption_dataset_no_outliers['score'] >= 0.6)]
    consumption_dataset_no_outliers.reset_index(inplace = True)
    consumption_dataset_no_outliers.drop(columns = ['index','score','level_0'],\
        inplace = True, axis = 1)

    os.remove('data/temp_consumption.txt')
    logging.debug('Anomalies in consumption dataset removed correctly!')

    return consumption_dataset_no_outliers


def transform_dataset5(initial_covid_dataset):
    ''' Performs the transformation of the dataset 5 '''

    # Dataset preprocessing
    initial_covid_dataset.drop(['geoid', 'countryterritorycode', 'continentexp', \
        'incidence', 'daterep', 'day'], inplace = True, axis = 1)

    # Null values treatment
    jpg11668_indexes = initial_covid_dataset[initial_covid_dataset\
        ['countriesandterritories'] == 'Cases_on_an_international_conveyance_Japan'].index
    wf_indexes = initial_covid_dataset[initial_covid_dataset\
        ['countriesandterritories'] == 'Wallis_and_Futuna'].index

    initial_covid_dataset.loc[jpg11668_indexes, 'popdata2019'] = 3711
    initial_covid_dataset.loc[wf_indexes, 'popdata2019'] = 11558

    # Feature type checking
    initial_covid_dataset['popdata2019'] = initial_covid_dataset['popdata2019'].astype('int64')

    # Fixing wrong values
    initial_covid_dataset['cases'] = initial_covid_dataset['cases'].abs()
    initial_covid_dataset['deaths'] = initial_covid_dataset['deaths'].abs()

    # Treatment of 2019 data
    initial_covid_dataset = initial_covid_dataset[initial_covid_dataset['year'] == 2020]

    # Outliers removal
    initial_covid_dataset['cases_index'] = initial_covid_dataset.apply\
        (lambda row: row.cases / row.popdata2019, axis = 1)
    initial_covid_dataset['deaths_index'] = initial_covid_dataset.apply\
        (lambda row: row.deaths / row.popdata2019, axis = 1)

    covid_dataset_no_outliers = initial_covid_dataset.drop(
        initial_covid_dataset.index[initial_covid_dataset['popdata2019'] > \
            initial_covid_dataset['popdata2019'].mean() + \
            2 * initial_covid_dataset['popdata2019'].std()]
    )

    covid_dataset_no_outliers = covid_dataset_no_outliers.drop(
        covid_dataset_no_outliers.index[covid_dataset_no_outliers['cases_index'] > \
            covid_dataset_no_outliers['cases_index'].mean() + \
            2 * covid_dataset_no_outliers['cases_index'].std()]
    )

    covid_dataset_no_outliers = covid_dataset_no_outliers.drop(
        covid_dataset_no_outliers.index[covid_dataset_no_outliers['deaths_index'] > \
            covid_dataset_no_outliers['deaths_index'].mean() + \
            2 * covid_dataset_no_outliers['deaths_index'].std()]
    )

    remove_covid_anomalies(covid_dataset_no_outliers)

    return covid_dataset_no_outliers


def remove_covid_anomalies(covid_dataset_no_outliers):
    ''' Removes the anomalies in the covid dataset using bigML '''

    api = BigML('sma_team_bgks', '4a3eda982d8771f3b55de8d617ad4c8334cfb401')

    covid_dataset_no_outliers.to_csv('data/temp_covid.txt')

    logging.debug('Connecting to bigML...')
    source = api.create_source('data/temp_covid.txt')
    api.ok(source)

    logging.debug('Adding covid dataset to bigML...')
    dataset = api.create_dataset(source)
    api.ok(dataset)

    logging.debug('Creating anomaly detector for covid dataset...')
    anomaly = api.create_anomaly(dataset)
    api.ok(anomaly)

    logging.debug('Detecting anomalies in covid dataset...')
    test_dataset = dataset
    batch_anomaly_score = api.create_batch_anomaly_score(anomaly, test_dataset)
    api.ok(batch_anomaly_score)
    api.download_batch_anomaly_score(batch_anomaly_score, filename='data/temp_covid.txt')

    anomaly_scores = pd.read_csv('data/temp_covid.txt')

    covid_dataset_no_outliers.reset_index(inplace = True)
    covid_dataset_no_outliers = covid_dataset_no_outliers.join(anomaly_scores)

    covid_dataset_no_outliers = covid_dataset_no_outliers\
        [~(covid_dataset_no_outliers['score'] >= 0.6)]
    covid_dataset_no_outliers.reset_index(inplace = True)
    covid_dataset_no_outliers.drop(columns = ['index','score','level_0'],\
        inplace = True, axis = 1)

    os.remove('data/temp_covid.txt')
    logging.debug('Anomalies in covid dataset removed correctly!')

    return covid_dataset_no_outliers


def obtain_enrichment_data(covid_dataset_no_outliers):
    ''' Obtains the enrichment data from the transformed covid dataset '''

    covid_dataset_no_outliers[['month', 'countriesandterritories','cases','deaths']]\
        .groupby(['month', 'countriesandterritories']).sum()

    covid_world = covid_dataset_no_outliers[['month','year', \
        'countriesandterritories','cases_index','deaths_index']]\
        .groupby(['month','year', 'countriesandterritories']).sum() * 10**5

    covid_world.rename(columns={"cases_index": "cummulative_cases_for_100k",\
        "deaths_index": "cummulative_deaths_for_100k"}, inplace = True)

    covid_world.reset_index(inplace= True)

    covid_to_join = covid_world[['month', 'year','cummulative_cases_for_100k',\
        'cummulative_deaths_for_100k']].groupby(['month', 'year']).mean()
    covid_to_join.reset_index(inplace=True)

    covid_to_join = covid_to_join[['month', 'year',	'cummulative_cases_for_100k']]

    return covid_to_join


def obtain_datacard_h1(consumption_dataset_no_outliers):
    ''' Method to obtain the first datacard '''

    datacard_h1 = consumption_dataset_no_outliers[consumption_dataset_no_outliers['year'] != 2020]
    datacard_h1 = datacard_h1[datacard_h1['product'] == 'SANDIA']
    datacard_h1.drop(columns=['product', 'market_penetration', 'ccaa'], inplace=True)

    datacard_h1.loc[datacard_h1.index, 'average_price_per_kg_or_l'] = \
        sklearn.preprocessing.minmax_scale(datacard_h1['average_price_per_kg_or_l'])
    datacard_h1.loc[datacard_h1.index, 'expenses_per_capita'] = \
        sklearn.preprocessing.minmax_scale(datacard_h1['expenses_per_capita'])
    datacard_h1.loc[datacard_h1.index, 'year'] = \
        sklearn.preprocessing.minmax_scale(datacard_h1['year'])

    return datacard_h1


def obtain_datacard_h2(consumption_dataset_no_outliers, covid_to_join):
    ''' Method to obtain the second datacard '''

    datacard_h2 = pd.merge(consumption_dataset_no_outliers,covid_to_join, on=['month','year'])
    datacard_h2 = datacard_h2[datacard_h2['product'] == 'SANDIA']
    datacard_h2.drop(columns=['product', 'year', 'market_penetration', 'ccaa'], inplace=True)

    datacard_h2.loc[datacard_h2.index, 'average_price_per_kg_or_l'] = \
        sklearn.preprocessing.minmax_scale(datacard_h2['average_price_per_kg_or_l'])
    datacard_h2.loc[datacard_h2.index, 'expenses_per_capita'] = \
        sklearn.preprocessing.minmax_scale(datacard_h2['expenses_per_capita'])
    datacard_h2.loc[datacard_h2.index, 'cummulative_cases_for_100k'] = \
        sklearn.preprocessing.minmax_scale(datacard_h2['cummulative_cases_for_100k'])

    return datacard_h2


def obtain_datacard_h3(consumption_dataset_no_outliers):
    ''' Method to obtain the third datacard '''

    datacard_h3 = consumption_dataset_no_outliers\
        [consumption_dataset_no_outliers['year'] != 2020]

    datacard_h3.loc[datacard_h3.index, 'average_price_per_kg_or_l'] = \
        sklearn.preprocessing.minmax_scale(datacard_h3['average_price_per_kg_or_l'])
    datacard_h3.loc[datacard_h3.index, 'expenses_per_capita'] = \
        sklearn.preprocessing.minmax_scale(datacard_h3['expenses_per_capita'])
    datacard_h3.loc[datacard_h3.index, 'market_penetration'] = \
        sklearn.preprocessing.minmax_scale(datacard_h3['market_penetration'])
    datacard_h3.loc[datacard_h3.index, 'year'] = \
        sklearn.preprocessing.minmax_scale(datacard_h3['year'])

    return datacard_h3


def transform_from_db():
    '''Method to retrive the data from the db'''

    try:
        db_conn = psycopg2.connect(
        database = AGRICULTURE_DATABASE,
        user = USERNAME,
        password = PASSWORD,
        host = HOST,
        port = PORT
        )

        db_conn.autocommit = True
        logging.debug('Retrieving data from the database...')

        dataframe_1 = pd.read_sql_query(SELECT_DATASET1, con = db_conn)
        consumption_transformed = transform_dataset1(dataframe_1)

        dataframe_5 = pd.read_sql_query(SELECT_DATASET5, con = db_conn)
        covid_transformed = transform_dataset5(dataframe_5)

        covid_to_join = obtain_enrichment_data(covid_transformed)

        datacard_h1 = obtain_datacard_h1(consumption_transformed)
        datacard_h2 = obtain_datacard_h2(consumption_transformed, covid_to_join)
        datacard_h3 = obtain_datacard_h3(consumption_transformed)

        datacard_h1.to_csv('data/datacard_h1.txt')
        datacard_h2.to_csv('data/datacard_h2.txt')
        datacard_h3.to_csv('data/datacard_h3.txt')

        logging.debug('Data transformed successfully!')
        db_conn.close()
    except Exception as exception:
        logging.error('Cannot retrieve the information from the database due \
        to this error: %s', exception)


if __name__ == '__main__':
    transform_from_db()
