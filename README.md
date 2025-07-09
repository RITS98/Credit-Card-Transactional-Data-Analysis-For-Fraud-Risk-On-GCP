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

4.  
