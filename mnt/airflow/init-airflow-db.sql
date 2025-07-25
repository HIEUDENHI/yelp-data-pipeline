CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE DATABASE metastore;
CREATE USER hive WITH ENCRYPTED PASSWORD 'hive';
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;