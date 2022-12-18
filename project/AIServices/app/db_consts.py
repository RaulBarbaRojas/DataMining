''' DATABASE NAMES '''

BASE_DATABASE = 'postgres'
AGRICULTURE_DATABASE = 'agriculture_db'


''' DATABASE ACCESS CONSTANTS '''

HOST = 'host.docker.internal'
PORT = '5432'

''' SQL: SELECTs'''
SELECT_CCAA = '''SELECT month,
ccaa,
product, 
AVG(market_penetration) as market_penetration,
AVG(average_price_per_kg_or_l) as average_price_per_kg_or_l
FROM agriculture_processed.datacard3 
'''

SELECT_NO_CCAA = '''SELECT month,
ccaa,
product, 
AVG(market_penetration) as market_penetration,
AVG(average_price_per_kg_or_l) as average_price_per_kg_or_l
FROM agriculture_processed.datacard3 
'''
