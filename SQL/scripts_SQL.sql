-- Banco de dados do webscraping
create USER webscraper WITH PASSWORD 'webscraper123';
create database WEBSCRAPING;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO webscraper;
GRANT ALL PRIVILEGES on database WEBSCRAPING to webscraper;

-- Banco de dados Airflow
create USER airflow WITH PASSWORD 'airflow123';
create database AIRFLOW;

GRANT ALL PRIVILEGES on database AIRFLOW to airflow;
