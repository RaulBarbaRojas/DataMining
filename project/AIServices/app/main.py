''' MODULE FOR IMPLEMENTING THE AI API OF THE PROJECT '''

import logging
from fastapi import FastAPI

from api_models import OptimalProduct
from api_models import OptimalProductCCAA
from api_models import SpecificProduct
from api_models import SpecificProductCCAA

logging.basicConfig(
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    level = logging.DEBUG
)

app = FastAPI()

@app.post('/predict-optimal-product/')
def handler_predict_optimal_product(product : OptimalProduct):
    ''' API handler for the optimal product prediction '''
    logging.debug('[SERVER]: received request for an optimal product prediction')
    logging.debug('[SERVER]: Year: %d',  product.month)
    logging.debug('[SERVER]: Month: %d',  product.year)


@app.post('/predict-optimal-product-by-ccaa/')
def handler_predict_optimal_product_ccaa(product : OptimalProductCCAA):
    ''' API handler for the optimal product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for an optimal product prediction ccaa')
    logging.debug('[SERVER]: Year: %d',  product.month)
    logging.debug('[SERVER]: Month: %d',  product.year)
    logging.debug('[SERVER]: CCAA: %s',  product.ccaa)


@app.post('/predict-product/')
def handler_predict_product(product : SpecificProduct):
    ''' API handler for specific product prediction '''
    logging.debug('[SERVER]: received request for a specific product prediction')
    logging.debug('[SERVER]: Year: %d',  product.month)
    logging.debug('[SERVER]: Month: %d',  product.year)
    logging.debug('[SERVER]: Product: %s',  product.product)


@app.post('/predict-product-by-ccaa/')
def handler_predict_product_ccaa(product : SpecificProductCCAA):
    ''' API handler for the specific product prediction for a given CCAA '''
    logging.debug('[SERVER]: received request for a specific product prediction ccaa')
    logging.debug('[SERVER]: Year: %d',  product.month)
    logging.debug('[SERVER]: Month: %d',  product.year)
    logging.debug('[SERVER]: Product: %s',  product.product)
    logging.debug('[SERVER]: CCAA: %s',  product.ccaa)
