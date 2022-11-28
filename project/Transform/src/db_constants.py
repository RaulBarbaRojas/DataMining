''' DATABASE NAMES '''

AGRICULTURE_DATABASE = 'agriculture_db'


''' USER CONSTANTS '''

USERNAME = 'postgres'
PASSWORD = 'postgres'


''' DATABASE ACCESS CONSTANTS '''

HOST = 'host.docker.internal'
PORT = '5432'


''' SQL: CREATE DATABASE '''

SELECT_DATASET1 = '''SELECT * FROM agriculture_raw.consumption'''
SELECT_DATASET5 = '''SELECT * FROM agriculture_raw.covid'''
