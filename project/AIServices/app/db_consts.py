''' DATABASE NAMES '''

BASE_DATABASE = 'postgres'
AGRICULTURE_DATABASE = 'agriculture_db'


''' DATABASE ACCESS CONSTANTS '''

HOST = 'host.docker.internal'
PORT = '5432'

''' SQL: SELECT DATACARD3'''
SELECT_DATACARD3 = '''SELECT * FROM agriculture_processed.datacard3'''
