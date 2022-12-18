''' MODULE FOR IMPLEMENTING THE API MODELS '''

import json

import pandas as pd
from pydantic import BaseModel
import psycopg2
import joblib
import numpy as np

from app.db_consts import AGRICULTURE_DATABASE
from app.db_consts import HOST
from app.db_consts import PORT
from app.db_consts import SELECT_CCAA
from app.db_consts import SELECT_NO_CCAA


def get_joblib_models():
    '''Loading the label encoder for products and regions.
        Loading the gradient boosting model'''
    encoder_regions = joblib.load('home/app/data/encoder_regions.joblib')
    encoder_products = joblib.load('home/app/data/encoder_products.joblib')
    gb_reg = joblib.load('home/app/data/gradient_boosting_model.joblib')

    return encoder_regions,encoder_products,gb_reg

def establish_db_connection():
    '''Creating psycopg2 connection'''
    with open('home/app/credentials.json', 'r', encoding = "utf8") as file:
        credentials = json.load(file)

    db_conn = psycopg2.connect(
    database = AGRICULTURE_DATABASE,
    user = credentials['username'],
    password = credentials['password'],
    host = HOST,
    port = PORT
    )
    return db_conn

def get_best_product(month):
    '''Returning the best product for given month'''
    db_conn = establish_db_connection()
    _, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True
    condition = f" WHERE month = {month} GROUP BY month, product, ccaa"
    dataframe = pd.read_sql_query(SELECT_NO_CCAA + condition , con = db_conn)
    db_conn.close()

    dataframe['results'] = model.predict(dataframe)
    df_pred = dataframe.groupby(['month','product'])[['product','results']].mean()
    product = df_pred.loc[df_pred['results'].idxmax(), 'product']
    return product_encoder.inverse_transform([int(product)])[0]



def get_best_product_for_ccaa(month, ccaa):
    '''Returning the best product for given month and region'''
    db_conn = establish_db_connection()
    region_enocoder, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True
    ccaa = region_enocoder.transform([ccaa])[0]
    condition = f" WHERE month = {month} AND ccaa={ccaa} GROUP BY month, product, ccaa"
    dataframe = pd.read_sql_query(SELECT_CCAA + condition , con = db_conn)
    db_conn.close()
    product = dataframe.loc[np.argmax(model.predict(dataframe))]['product']
    return product_encoder.inverse_transform([int(product)])[0]

def get_specific_product_prediction(month, product):
    '''Returning returning predicted expenses per capita for given product and month'''
    db_conn = establish_db_connection()
    _, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True

    product = product_encoder.transform([product])[0]
    condition = f''' WHERE month = {month} AND product = {product}
                    GROUP BY month, product, ccaa'''
    dataframe  = pd.read_sql_query(SELECT_CCAA + condition , con = db_conn)
    db_conn.close()
    return np.average(model.predict(dataframe))

def get_specific_product_prediction_for_ccaa(month, ccaa, product):
    '''Returning returning predicted expenses per capita for given product, month and region'''
    db_conn = establish_db_connection()
    region_enocoder, product_encoder, model = get_joblib_models()
    db_conn.autocommit = True
    ccaa = region_enocoder.transform([ccaa])[0]
    product = product_encoder.transform([product])[0]
    condition = f''' WHERE month = {month} AND ccaa={ccaa} AND product = {product}
                    GROUP BY month, product, ccaa'''
    dataframe  = pd.read_sql_query(SELECT_CCAA + condition , con = db_conn)
    db_conn.close()
    return model.predict(dataframe )[0]

class OptimalProduct(BaseModel):
    ''' Class that represents the base model for the optimal product post request '''

    month : int
    year : int



class OptimalProductCCAA(BaseModel):
    ''' Class that represents the base model for the optimal product by ccaa post request '''

    month : int
    year : int
    ccaa : str


class SpecificProduct(BaseModel):
    ''' Class that represents the base model for the optimal product by ccaa post request '''

    month : int
    year : int
    product : str


class SpecificProductCCAA(BaseModel):
    ''' Class that represents the base model for the optimal product by ccaa post request '''

    month : int
    year : int
    ccaa : str
    product : str
