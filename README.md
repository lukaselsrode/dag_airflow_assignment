# DAG Airflow Assignment

## Overview

This assignment involves a three-step data processing workflow using Apache Airflow:

1. **Scraping URLs**: Download links are extracted from the provided URL.
2. **Data Staging**: Downloads are stored in a Google Cloud Storage (GCS) bucket, which serves as a staging area. Additionally, there's a potential to handle exceptions for data in the staging area locally.
3. **Data Ingestion**: The staged data is ingested into BigQuery from the GCS bucket.

## Visual Representation

![](misc/visual_repr.png)

## Scraping URL for Links

Initially, the plan was to use the `requests` or `BeautifulSoup` (bs4) libraries for URL scraping. However, due to the webpage's dynamic content loaded with webpack, Selenium was chosen to render the webpage and extract necessary links from the XML content. The links under the 'Business' header were manually inspected and selected.

## Downloading and Uploading Links into DataStore (GCS)

The initial approach was to stage files locally in the `tmp/` directory. This was later deemed inefficient since the files were meant to be uploaded eventually. A shift was made to use an IO stream, allowing the content from the download URLs to be written directly to a file buffer and uploaded to GCS. This decision was influenced by the need to manually inspect files in GCS should issues arise with the BigQuery table.

## Ingesting Links to BigQuery from GCS

The initial strategy involved storing downloaded GCS links as compressed `.gz` files, considering BigQuery's native support for this format. However, the discovery of various file formats like `.zip`, `.csv`, and `.xlsx`, and the occasional inability of BigQuery's `JobConfig()` to infer schemas correctly, necessitated a dual approach to handle exceptions. This involved either converting files to `.csv` format before replacing them in GCS or manually creating table schemas for BigQuery ingestion.

## Running the Program

### Installation

Clone the repository and navigate into the project directory:


```bash
git clone https://github.com/lukaselsrode/dag_airflow_assignment.git
cd dag_airflow_assignment
```

### Setting Up the Environment and Dependencies

Create and activate a virtual environment, then install the required dependencies:

```bash
python3 -m venv .env/ 
source .env/bin/activate
pip install --upgrade pip  # airflow dependencies can be annoying
pip install -r requirements.txt
```

### Selenium Chrome Driver Requirement

This script utilizes Selenium and requires the Chrome Driver. Ensure the Chrome Driver is installed and configured: [Chrome WebDriver](https://sites.google.com/chromium.org/driver/).

### Google Service Account Secret JSON File

Execution requires access to the Google Cloud `nz_business` project. Create a service account, download the secret JSON file, and place it in your project directory. Update the following line in `/dag/nz_business_pipeline.py` to reference your service account credentials:
```python 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../.secrete.json" # path to your google service account credentials
```
### Execution 
#### Demo
PUT VIDEO/GIF HERE
#### From Python
To execute the ingestion pipeline from the command line, 
make sure you remove the un-comment section of code in the /dag/nz_buisiness_pipeline.py folder. This was commented out to test the 
nz_business_dag.py file. 
```python
"""
def main():
    stage_files_in_gcs()
    ingest_data_from_storage_to_bigquery()

if __name__ == "__main__":
    main()
"""
```
You can now directly execute the ingestion pipeline from the command line with. 

```bash 
cd dags
python3 nz_business_pipeline.py
```
#### From Airflow
While experimenting with `docker-compose.yml` and attempting to set up a webserver, it was found that the `airflowctl` project provided a viable means for local testing. Note that the virtual environment may need reconfiguration for this testing method: [airflowctl GitHub Repository](https://github.com/kaxil/airflowctl).
