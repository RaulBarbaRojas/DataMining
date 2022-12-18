''' MODULE FOR IMPLEMENTING THE API MODELS '''

from pydantic import BaseModel



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
