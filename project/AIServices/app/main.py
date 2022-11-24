''' MODULE FOR IMPLEMENTING THE AI API OF THE PROJECT '''

import logging
from fastapi import FastAPI

logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)

app = FastAPI()

@app.post('/predict-optimal-product/')
def handler_predict_optimal_product():
    ''' API handler for the optimal product prediction '''
    logging.debug('[SERVER]: received request for an optimal product prediction')


@app.post('/predict-optimal-product-by-ccaa')
def handler_predict_optimal_product_ccaa():
    ''' API handler for the optimal product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for an optimal product prediction ccaa')


@app.post('/predict-product')
def handler_predict_product():
    ''' API handler for specific product prediction '''
    logging.debug('[SERVER]: received request for a specific product prediction')
