# Credit Card Transactional Data Analysis For Fraud Risk On GCP


## Technology Used
- Python
- PySpark
- Google Storage
- GCP Dataproc Serverless
- GCP BigQuery
- GCP Composer (Apache Airflow)
- PyTest
- GitHub
- GitHub Actions (For CI/CD)

## Architecture Diagram

![Credit Fraud](https://github.com/user-attachments/assets/7b836bea-ba7e-4246-9811-06fa2538fc34)

## Steps

### Creating Buckets For Data Storage

1. First Create a bucket which acts as placeholder for the data

<img width="1040" alt="image" src="https://github.com/user-attachments/assets/5e69bf57-edd4-46eb-98e4-a859407cff40" />

2.  Create folders as per the architeccture diagram

<img width="770" alt="image" src="https://github.com/user-attachments/assets/452aa659-3a16-47ab-a0b3-e6e66c88e102" />

3.  Upload the inital `cardholders.csv` file to the bucket so that the table can be created inside the BigQuery

<img width="830" alt="image" src="https://github.com/user-attachments/assets/4d32f0a7-56f5-49d8-b3e0-a79fe81ab4af" />

### Creating Datasets and Tables in BigQuery

1. Create a Dataset (which is basically a database or datawarehouse metaphorically)

<img width="1568" alt="image" src="https://github.com/user-attachments/assets/9516aa3c-9fd2-4b2f-a86b-ed6d42ba44a0" />

2. Create Cardholders external table in Bigquery

<img width="999" alt="image" src="https://github.com/user-attachments/assets/7ecec57f-827f-4b37-8ab7-7a8572721a74" />

3. Other tables with be created by Airflow if not present

### Create a Airflow Instance

1. Go to GCP Composer and create a Airflow instance. Choose the latest version of airflow.

<img width="486" alt="image" src="https://github.com/user-attachments/assets/76660116-6314-478f-af44-23ac71c5724a" />

2. Click on `Create` button. It takes some time (around 10-15 minutes to create and load the instance)
3. Upload the day python file by clickin on the `DAGs` link and then `OPEN DAGS FOLDER` option.

<img width="1202" alt="image" src="https://github.com/user-attachments/assets/e4125620-191d-4f60-97fb-5dfd9bf01273" />

<img width="862" alt="image" src="https://github.com/user-attachments/assets/72d4c2e0-b702-40f3-b3b9-8bad97fc5448" />

<img width="783" alt="image" src="https://github.com/user-attachments/assets/6f7081c2-9860-48f7-99c8-452fcffea679" />

4. Below is the Airflow UI
<img width="1687" alt="image" src="https://github.com/user-attachments/assets/97530068-aaf1-434e-a5f6-f854cd3a9c7b" />


### Creating Dataproc Cluster







