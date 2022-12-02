----- DATA EXTRACTION SETUP -----

The result of the data extraction will be text files with the extracted data.
After it, the data load will take place, which includes two dabase schemas:

 1. agriculture_raw : the schema that contains the tables where raw data will be stored.
 2. agriculture_processed: the schema that contains the tables where processed data will be stored. 

Steps:

 1. Create docker image: docker build -t <docker-image-name> .
 2. Create a postgres database inside a docker container: docker run --name <name-of-db-container> -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
 3. Create docker container: docker run --name <container-name> -v <path-to-your-system-location-data-storage>:/home/app/data -d <docker-image-name>
