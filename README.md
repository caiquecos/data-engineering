ANP Fuel Sales ETL Test
=======================

This repository consists in developing in docker, airflow and postgresql an ETL pipeline to extract internal pivot caches from consolidated reports [made available](http://www.anp.gov.br/dados-estatisticos) by Brazilian government's regulatory agency for oil/fuels, *ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)*.


To execute the execution, open your IDE and the challenger folder, after opening, run the command docker-compose up.

To view the DAG created in the project, access http://localhost:8080/ and view the username and password in the docker-compose.yml file.

To view the data entered in the table, access PgAdmin http://localhost:15432/ and register the connection by inserting the host, username and password according to the postgres configuration in docker-compose.yml file.

# Obs

To run this repository it is necessary to have docker and docker compose installed on your machine.
