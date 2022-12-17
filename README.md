# Introduction

This repository contains all the code and documentation associated to the project of the course of Data Mining 2022-2023.

# Authors

The authors of this project are:


<table>
  <thead>
    <th>Name</th>
    <th>Github</th>
    <th>E-Mail</th>
  </thead>
  <tbody>
    <tr>
      <td>Raúl Barba Rojas</td>
      <td><a href="https://github.com/RaulBarbaRojas">RaulBarbaRojas</a></td>
      <td><a href="mailto:Raul.Barba@alu.uclm.es">Raul.Barba@alu.uclm.es</a></td>
    </tr>
    <tr>
      <td>Diego Guerrero Del Pozo</td>
      <td><a href="https://github.com/DiegoGDP">DiegoGDP</a></td>
      <td><a href="mailto:Diego.Guerrero@alu.uclm.es">Diego.Guerrero@alu.uclm.es</a></td>
    </tr>
    <tr>
      <td>Maja Anna Swierk</td>
      <td><a href="https://github.com/manna8">Manna8</a></td>
      <td><a href="mailto:MajaAnna.Swierk@alu.uclm.es">MajaAnna.Swierk@alu.uclm.es</a></td>
    </tr>
    <tr>
      <td>Jakub Konieczny</td>
      <td><a href="https://github.com/KubaKonieczny">KubaKonieczny</a></td>
      <td><a href="mailto:Jakub.Konieczny@alu.uclm.es">Jakub.Konieczny@alu.uclm.es</a></td>
    </tr>
  </tbody>
</table>


# Setup

## Architecture setup guide

Step 1. Unzip the code and move to the directory <directory of the project code>/project/
Step 2. Perform the steps of the setup for the database.
Step 3. Perform the steps of the setup for the extract container.
Step 4. Perform the steps of the setup for the transform and load container.
Step 5. Perform the steps of the setup for the extracted data quality container.
Step 6. Perform the steps of the setup for the transformed data quality container.
Step 7. Perform the steps of the setup for the API container.

## Database setup guide

Step 1. docker run --name <name of the DB Container> -d -e POSTGRES_PASSWORD=postgres postgres

## Extract container setup guide

Step 1. cd <directory of the project code>/project/Extract/
Step 2. docker build -t extractimage .
Step 3. docker run --name extractcontainer -d extractimage

## Load and transform container setup guide

This container uses a shared folder to store joblib files associated to the transformation of the data. Thus, the shared folder must be created when dockerizing.

Step 1. cd <directory of the project code>/project/Transform_Load/
Step 2. docker build -t loadimage .
Step 3. docker run --name loadcontainer -d -v <directory of the project code>/project/Transform_Load/data:/home/app/data loadimage

## Extracted data quality container setup guide

Step 1. cd <directory of the project code>/project/ExtractedDataQuality/
Step 2. docker build -t extractdataqualityimage .
Step 3. docker run --name extractdataqualitycontainer -d extractdataqualityimage

## Transformed data quality container setup guide

Step 1. cd <directory of the project code>/project/TransformDataQuality/
Step 2. docker build -t transformdataqualityimage .
Step 3. docker run --name transformdataqualitycontainer -d transformdataqualityimage

## API container setup guide

IMPORTANT: the project zip contains a folder called "models", you must copy the files in the shared folder of the docker container (so that it can use the models!)

Step 1. cd <directory of the project code>/project/AIServices/
Step 2. Copy the files of the folder "models" (in the root directory of the zip) and paste them inside <directory of the project code>/project/AIServices/app/data/
Step 3. docker build -t apiimage .
Step 4. docker run --name apicontainer -d -v <directory of the project code>/project/AIServices/app/data/:/home/app/data apiimage