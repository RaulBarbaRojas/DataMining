----- DATA EXTRACTION SETUP -----

The result of the data extraction will be text files with the extracted data.
After it, the data load will take place, which includes two dabase schemas:

 1. agriculture_raw : the schema that contains the tables where raw data will be stored.
 2. agriculture_processed: the schema that contains the tables where processed data will be stored. 

Steps:

 1. Create docker image: docker build -t <docker-image-name> .
 2. Create a postgres database inside a docker container: docker run --name <name-of-db-container> -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
 3. Create docker container: docker run --name <container-name> -d <docker-image-name>

Example:

 1. Execute: docker build -t extract_image .
 2. Execute: docker run --name postgres_container -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
 3. Execute: docker run --name extract_container -d extract_image