''' MODULE FOR IMPLEMENTING THE AI API OF THE PROJECT '''

import logging
import json

import pandas as pd

from fastapi import FastAPI # pylint: disable=import-error
import psycopg2
import joblib
import numpy as np

from app.api_models import OptimalProduct
from app.api_models import OptimalProductCCAA
from app.api_models import SpecificProduct
from app.api_models import SpecificProductCCAA


from app.db_consts import AGRICULTURE_DATABASE
from app.db_consts import HOST
from app.db_consts import PORT
from app.db_consts import SELECT_CCAA
from app.db_consts import SELECT_NO_CCAA


logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)


app = FastAPI()

@app.post('/predict-optimal-product/')
def handler_predict_optimal_product(product : OptimalProduct):
    ''' API handler for the optimal product prediction '''
    logging.debug('[SERVER]: received request for an optimal product prediction')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    db_conn = establish_db_connection()
    _, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True
    condition = f" WHERE month = {product.month} GROUP BY month, product, ccaa"
    dataframe = pd.read_sql_query(SELECT_NO_CCAA + condition , con = db_conn)
    db_conn.close()

    dataframe['results'] = model.predict(dataframe)
    df_pred = dataframe.groupby(['month','product'])[['product','results']].mean()
    product = df_pred.loc[df_pred['results'].idxmax(), 'product']
    best = product_encoder.inverse_transform([int(product)])[0]
    return {'best_product':best}


@app.post('/predict-optimal-product-by-ccaa/')
def handler_predict_optimal_product_ccaa(product : OptimalProductCCAA):
    ''' API handler for the optimal product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for an optimal product prediction ccaa')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    logging.debug('[SERVER]: CCAA: %s',  product.ccaa)
    db_conn = establish_db_connection()
    region_enocoder, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True
    ccaa = region_enocoder.transform([product.ccaa])[0]
    condition = f" WHERE month = {product.month} AND ccaa={ccaa} GROUP BY month, product, ccaa"
    dataframe = pd.read_sql_query(SELECT_CCAA + condition , con = db_conn)
    db_conn.close()
    product = dataframe.loc[np.argmax(model.predict(dataframe))]['product']
    best = product_encoder.inverse_transform([int(product)])[0]
    return  {'best_product':best}


@app.post('/predict-product/')
def handler_predict_product(product : SpecificProduct):
    ''' API handler for specific product prediction '''
    logging.debug('[SERVER]: received request for a specific product prediction')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    logging.debug('[SERVER]: Product: %s',  product.product)
    db_conn = establish_db_connection()
    _, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True

    item = product_encoder.transform([product.product])[0]
    condition = f''' WHERE month = {product.month} AND product = {item}
                    GROUP BY month, product, ccaa'''
    dataframe  = pd.read_sql_query(SELECT_CCAA + condition , con = db_conn)
    db_conn.close()
    value = np.average(model.predict(dataframe))
    return {'value':value}


@app.post('/predict-product-by-ccaa/')
def handler_predict_product_ccaa(product : SpecificProductCCAA):
    ''' API handler for the specific product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for a specific product prediction ccaa')
    logging.debug('[SERVER]: Year: %d',  product.year)
    logging.debug('[SERVER]: Month: %d',  product.month)
    logging.debug('[SERVER]: Product: %s',  product.product)
    logging.debug('[SERVER]: CCAA: %s',  product.ccaa)
    db_conn = establish_db_connection()
    region_enocoder, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True
    ccaa = region_enocoder.transform([product.ccaa])[0]
    item = product_encoder.transform([product.product])[0]
    condition = f''' WHERE month = {product.month} AND ccaa={ccaa} AND product = {item}
                    GROUP BY month, product, ccaa'''
    dataframe  = pd.read_sql_query(SELECT_CCAA + condition , con = db_conn)
    db_conn.close()
    value = model.predict(dataframe)[0]
    return {'value':value}


def get_joblib_models():
    '''Loading the label encoder for products and regions.
        Loading the gradient boosting model'''
    encoder_regions = joblib.load('/home/app/data/encoder_regions.joblib')
    encoder_products = joblib.load('/home/app/data/encoder_products.joblib')
    gb_reg = joblib.load('/home/app/data/gradient_boosting_model.joblib')

    return encoder_regions,encoder_products,gb_reg

def establish_db_connection():
    '''Creating psycopg2 connection'''
    with open('/home/app/data/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = AGRICULTURE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )
    return db_conn
