''' DATABASE NAMES '''

BASE_DATABASE = 'postgres'
RAW_DATABASE = 'agriculture_raw'
PROCESSED_DATABASE = 'agriculture_processed'

''' USER CONSTANTS '''

USERNAME = 'postgres'
PASSWORD = 'postgres'

''' DATABASE ACCESS CONSTANTS '''

HOST = '127.0.0.1'
PORT = '5432'

''' SQL: CREATE RAW SCHEMA (IF NOT EXISTS) '''
DROP_RAW_SCHEMA_SQL = 'DROP DATABASE IF EXISTS agriculture_raw'
RAW_SCHEMA_SQL = 'CREATE DATABASE agriculture_raw'