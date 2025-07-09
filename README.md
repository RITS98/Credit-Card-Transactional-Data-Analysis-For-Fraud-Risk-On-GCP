# Credit Card Transactional Data Analysis For Fraud Risk On GCP

This project aims to analyze credit card transactional data to identify potential fraud risks using Google Cloud Platform (GCP) services. The solution leverages PySpark for data processing, Airflow for orchestration, and BigQuery for data storage and querying.

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

## Dataflow

1. **Data Ingestion**: Credit card transaction data is uploaded to a Google Cloud Storage (GCS) bucket.
2. **Data Processing**:
   - A PySpark job is triggered to process the transaction data.
   - The job reads the data from GCS, processes it, and writes the results to a BigQuery table.
3. **Data Orchestration**: Apache Airflow (GCP Composer) orchestrates the workflow, managing the scheduling and execution of the PySpark job.
4. **Data Storage**: Processed data is stored in BigQuery for further analysis and reporting.
5. **Data Archiving**: Processed JSON files are moved to an archive folder in GCS for long-term storage.
6. **Testing**: Unit tests are written using PyTest to ensure the correctness of the PySpark job.
7. **CI/CD Pipeline**: GitHub Actions is used to automate testing and deployment of the Airflow DAG and PySpark job.

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

5. List of connections

- Airflow connections are used to connect to external systems like GCS, BigQuery, etc. Below is the list of connections used in this project.

<img width="995" alt="image" src="https://github.com/user-attachments/assets/2ef7d1f0-b6a1-4cfb-b27b-544e1ec9196d" />

### Creating Dataproc Cluster

1.  Enable IAM permissions (You need roles like Dataproc Admin, Compute Admin, Service Account User, Storage Admin)
2.  Default Compute Engine Service Account must exist with appropriate permissions
3.  Go to Dataproc
    - Navigate to Dataproc page in the GCP Console.

4. Click “Create Cluster”
5. Choose the Cluster Type
   - Standard (default) – for general use
   - Single Node – for dev/testing
   - High Availability – for production

6. Configure the Cluster
   - Cluster Name: e.g., my-dataproc-cluster
   - Region: e.g., us-central1
   - Zone: Optional, auto-selected from region 
   - Cluster mode: Standard, HA, or Single Node

7. Node Configuration
   - Master Node: Machine type (e.g., n1-standard-2), disk size
   - Worker Nodes: Number of workers, machine type, disk size

7. Click “Create”
   - Wait a few minutes for the cluster to be provisioned.

<img width="824" alt="image" src="https://github.com/user-attachments/assets/840ac888-9022-4259-99ea-2c804a13b5f7" />


### CI/CD Pipeline (GitHub Actions)

This project uses **GitHub Actions** to automate Continuous Integration (CI) and Continuous Deployment (CD) for different branches (`dev` and `main`). Below is a detailed breakdown of the workflow configuration.

#### Trigger Conditions

The workflow is triggered on **push events** to the following branches:

```yaml
on:
  push:
    branches:
      - dev
      - main
```

#### Job 1: Run Tests on Dev Branch (`dev`)

##### Purpose:

To run unit tests (specifically `test_transactions_processing.py`) when changes are pushed to the `dev` branch.

##### Steps:

1. **Checkout Code**

   ```yaml
   - name: Checkout Code
     uses: actions/checkout@v3
   ```

   This step pulls your repository's code into the GitHub runner so the next steps can access it.

2. **Set Up Python Environment**

   ```yaml
   - name: Set Up Python
     uses: actions/setup-python@v3
     with:
       python-version: "3.11"
   ```

   It installs Python 3.11 so the testing can run in the expected Python environment.

3. **Install Dependencies**

   ```yaml
   - name: Install Dependencies
     run: |
       pip install -r requirements.txt
   ```

   Installs all required Python packages listed in `requirements.txt`.

4. **Run Pytest for Specific Test File**

   ```yaml
   - name: Run Pytest for `test_transactions_processing.py`
     run: pytest tests/test_transactions_processing.py
   ```

   Executes only the `test_transactions_processing.py` file to validate transaction processing logic during development.


#### Job 2: Deploy to Production (Main Branch Only)

##### Purpose:

When changes are merged into the `main` branch, this job automates deployment by uploading:

* The PySpark job to Google Cloud Storage (GCS)
* The Airflow DAG to Cloud Composer (Airflow on GCP)

##### Steps:

1. **Checkout Code**

   ```yaml
   - name: Checkout Code
     uses: actions/checkout@v3
   ```

   Same as in the test job, this fetches the latest code.

2. **Authenticate to Google Cloud Platform**

   ```yaml
   - name: Authenticate to GCP
     uses: google-github-actions/auth@v1
     with:
       credentials_json: ${{ secrets.GCP_SA_KEY }}
   ```

   Authenticates using a GCP Service Account. The credentials are stored securely in GitHub Secrets as `GCP_SA_KEY`.

3. **Setup Google Cloud SDK**

   ```yaml
   - name: Setup Google Cloud SDK
     uses: google-github-actions/setup-gcloud@v1
     with:
       project_id: ${{ secrets.GCP_PROJECT_ID }}
   ```

   Installs and configures the `gcloud` CLI to work with the GCP project defined in `GCP_PROJECT_ID`.

4. **Upload PySpark Job to GCS**

   ```yaml
   - name: Upload Spark Job to GCS
     run: |
       gsutil cp spark_job.py gs://credit-card-data-analysis-ritayan/spark_job/
   ```

   Uploads the PySpark job file (`spark_job.py`) to a GCS bucket where Dataproc can access it.

5. **Upload Airflow DAG to Composer**

   ```yaml
   - name: Upload Airflow DAG to Composer
     run: |
       gcloud composer environments storage dags import \
         --environment airflow-cluster \
         --location us-east1 \
         --source airflow_job.py
   ```

   Uploads the DAG file (`airflow_job.py`) into the Composer environment's DAGs folder so it can be scheduled and run in production.


#### Secrets Used

The workflow relies on GitHub Secrets for secure authentication:

| Secret Name      | Description                            |
| ---------------- | -------------------------------------- |
| `GCP_SA_KEY`     | GCP Service Account credentials (JSON) |
| `GCP_PROJECT_ID` | Google Cloud Project ID                |

**How to add Github Secrets ?**
1. Navigate to the repository: Go to the main page of your GitHub repository. 
2. Access Settings: Click on the "Settings" tab at the top of the repository. 
3. Locate Secrets and Variables: In the left sidebar, scroll down and click on "Secrets and variables" under "Security".

<img width="1344" alt="image" src="https://github.com/user-attachments/assets/30b7901f-2d40-47d8-8c05-380f80212828" />

4. Choose Actions: Select "Actions" to manage secrets specifically for GitHub Actions workflows. 
5. Create a New Secret: Click on "New repository secret".

<img width="1170" alt="image" src="https://github.com/user-attachments/assets/8bd21113-3bc1-4d80-9c8f-bb4afbdcfeb3" />

6. Name and Value: Enter a name for your secret (using all caps is a common convention) and then input the actual secret value. 
7. Add the Secret: Click the "Add secret" button to save the secret. 

<img width="1179" alt="image" src="https://github.com/user-attachments/assets/b3c026f7-2260-48e1-9b39-d67e6dbcafca" />


## Results

1. DAGs in the Airflow Cluster

<img width="1591" alt="image" src="https://github.com/user-attachments/assets/17585413-0587-420e-85e8-4aadbad1533e" />

2. The pipeline

<img width="1588" alt="image" src="https://github.com/user-attachments/assets/177dd714-fc85-4f9f-8f2a-a35a5dcffa4a" />

3. Putting the transaction files in the transaction folder

<img width="1205" alt="image" src="https://github.com/user-attachments/assets/ab349936-9943-43b2-a7b9-2c438d9241ce" />

4. The spark is triggered and you can see the spark job id created using the uuid

<img width="1106" alt="image" src="https://github.com/user-attachments/assets/ab20605c-8a56-4c3f-be32-0e61258741e5" />

5. The spark job is running in the Dataproc Cluster

<img width="1165" alt="image" src="https://github.com/user-attachments/assets/dfec75d6-5936-4e09-be32-ef80adf31b97" />

6. The job is succeeded. 

<img width="1146" alt="image" src="https://github.com/user-attachments/assets/9324174c-28e1-474d-a0a6-17d45193dd5a" />

<img width="1128" alt="image" src="https://github.com/user-attachments/assets/80be42ea-1955-4908-ab4c-0696022a2fd5" />

7. After processing the json file is moved to archieve area.

<img width="742" alt="image" src="https://github.com/user-attachments/assets/41ed92aa-6125-486b-9e64-be98d9991b83" />

8. The transaction table is populated with the data.

<img width="1058" alt="image" src="https://github.com/user-attachments/assets/95a7f443-7316-498a-81ba-ed7c910b7d66" />




