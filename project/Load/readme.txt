SETUP:

----- DATA LOAD SETUP -----

The result of the data load will include two dabase schemas:

 1. agriculture_raw : the schema that contains the tables where raw data will be stored.
 2. agriculture_processed: the schema that contains the tables where processed data will be stored. 
 
Steps:

 1. Create a postgres database inside a docker container: 
 2. Create a docker container that will create (drop if exists) the tables and add the extracted data to them.