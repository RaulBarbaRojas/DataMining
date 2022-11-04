SETUP:

----- DATA EXTRACTION SETUP -----

The result of the data extraction will be text files with the extracted data. Steps:

 1. Create docker image: docker build -t <docker-image-name> .
 2. Create docker container: docker run --name <container-name> -v <path-to-your-system-location-data-storage>:/home/app/data -d extract
