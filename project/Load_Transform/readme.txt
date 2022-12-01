SETUP:

----- DATA LOAD & TRANSFORMATION SETUP -----

The result of the data load will include two dabase schemas:

 1. agriculture_raw : the schema that contains the tables where raw data will be stored.
 2. agriculture_processed: the schema that contains the tables where processed data will be stored. 
 
Steps:

 1. Create a postgres database inside a docker container: docker run --name <name-of-db-container> -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
 2. Create a docker container that will create (drop if exists) the tables and add the extracted data to them: docker run --name <container-name> -v <path-to-your-system-location-data-storage>:/home/app/data -d load_transform