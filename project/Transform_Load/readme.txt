SETUP:

----- DATA TRANSFORMATION SETUP -----
 
Steps:

 1. Create docker image: docker build -t <docker-image-name> .
 2. Create docker container: docker run --name <container-name> -v <path-to-your-system-location-data-storage>:/home/app/data -d <docker-image-name>