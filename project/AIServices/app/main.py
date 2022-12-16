''' MODULE FOR IMPLEMENTING THE AI API OF THE PROJECT '''

import logging
import json

import pandas as pd
from fastapi import FastAPI

import psycopg2
import joblib

from api_models import OptimalProduct
from api_models import OptimalProductCCAA
from api_models import SpecificProduct
from api_models import SpecificProductCCAA

from db_consts import AGRICULTURE_DATABASE
from db_consts import HOST
from db_consts import PORT
from db_consts import SELECT_DATACARD3

logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)

def get_joblib_models():
    '''Loading the label encoder for products and regions.
        Loading the gradient boosting model'''
    encoder_regions = joblib.load('/home/app/data/encoder_regions.joblib')
    encoder_products = joblib.load('/home/app/data/encoder_products.joblib')
    gb_reg = joblib.load('/home/app/data/gradient_boosting_model.joblib')

    return encoder_regions,encoder_products,gb_reg

def load_datacard3_to_dataframe():
    '''Loading datacard 3 from database to dataframe'''
    with open('/home/app/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = AGRICULTURE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )

    db_conn.autocommit = True
    logging.debug('Retrieving data from the database...')

    datacard3 = pd.read_sql_query(SELECT_DATACARD3, con = db_conn)

    logging.debug('Data dowloaded successfully!')
    db_conn.close()
    return datacard3


def get_best_product_with_value(encoder_products, gb_reg, df_for_prediction):
    '''Returning product with highest expected expenses per capita'''
    predictions = {}
    for index, row in  df_for_prediction.iterrows():
        predictions[index] = gb_reg.predict([row])
    best = max(predictions, key = predictions.get)
    best_value=max(predictions.values())

    best_product = df_for_prediction.loc[best,['product']]
    best_product = encoder_products.inverse_transform([int(best_product[0])])
    return best_product[0], best_value[0]

app = FastAPI()

@app.post('/predict-optimal-product/')
def handler_predict_optimal_product(product : OptimalProduct):
    ''' API handler for the optimal product prediction '''
    logging.debug('[SERVER]: received request for an optimal product prediction')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)

    _, encoder_products, gb_reg = get_joblib_models()
    datacard3 = load_datacard3_to_dataframe()
    features = ['month','ccaa', 'product', 'market_penetration', 'average_price_per_kg_or_l']
    df_for_prediction = datacard3[features][(datacard3['month'] == product.month)]
    best_product, _ = get_best_product_with_value(encoder_products, gb_reg, df_for_prediction)

    return best_product


@app.post('/predict-optimal-product-by-ccaa/')
def handler_predict_optimal_product_ccaa(product : OptimalProductCCAA):
    ''' API handler for the optimal product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for an optimal product prediction ccaa')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    logging.debug('[SERVER]: CCAA: %s',  product.ccaa)

    encoder_regions, encoder_products, gb_reg = get_joblib_models()
    datacard3 = load_datacard3_to_dataframe()
    features = ['month','ccaa', 'product', 'market_penetration', 'average_price_per_kg_or_l']
    region = encoder_regions.transform([product.ccaa])
    df_for_prediction = datacard3[features][(datacard3['month'] == product.month)
                                            & (datacard3['ccaa']==region[0])]
    best_product, _ = get_best_product_with_value(encoder_products, gb_reg, df_for_prediction)

    return best_product


@app.post('/predict-product/')
def handler_predict_product(product : SpecificProduct):
    ''' API handler for specific product prediction '''
    logging.debug('[SERVER]: received request for a specific product prediction')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    logging.debug('[SERVER]: Product: %s',  product.product)

    _, encoder_products, gb_reg = get_joblib_models()

    datacard3 = load_datacard3_to_dataframe()
    features = ['month','ccaa', 'product', 'market_penetration', 'average_price_per_kg_or_l']

    item = encoder_products.transform([product.product])
    df_for_prediction = datacard3[features][(datacard3['month'] == product.month)
                                        & (datacard3['product']==item[0]) ]

    _, best_value = get_best_product_with_value(encoder_products, gb_reg, df_for_prediction)

    return  best_value


@app.post('/predict-product-by-ccaa/')
def handler_predict_product_ccaa(product : SpecificProductCCAA):
    ''' API handler for the specific product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for a specific product prediction ccaa')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    logging.debug('[SERVER]: Product: %s',  product.product)
    logging.debug('[SERVER]: CCAA: %s',  product.ccaa)

    encoder_regions, encoder_products, gb_reg = get_joblib_models()

    datacard3 = load_datacard3_to_dataframe()
    features = ['month','ccaa', 'product', 'market_penetration', 'average_price_per_kg_or_l']

    item = encoder_products.transform([product.product])
    region = encoder_regions.transform([product.ccaa])

    df_for_prediction = datacard3[features][
                    (datacard3['month'] == product.month) &
                    (datacard3['product']==item[0]) &
                    (datacard3['ccaa']==region[0]) ]
    _, best_value = get_best_product_with_value(encoder_products, gb_reg, df_for_prediction)

    return best_value


# import warnings
# warnings.filterwarnings("ignore")


# print(handler_predict_optimal_product(OptimalProduct(month = 9, year = 2019)))
# print(handler_predict_optimal_product_ccaa(OptimalProductCCAA(month = 8,
#                                                             year = 2019,
#                                                             ccaa='Murcia')))
# print(handler_predict_product(SpecificProduct(month = 1, year = 2019, product='TOMATES')))
# print(handler_predict_product_ccaa(SpecificProductCCAA(month = 1,
#                                                         year = 2019,
#                                                         product='TOMATES',
#                                                         ccaa='Murcia')))
