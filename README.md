# DAG Airflow Assignment
### Overview
The Program involves 3 main components:
 1. Scraping the url provided for the links to download
 2. storing the download in a GCS bucket to serve as a staging area
    2'. Handle Exceptions of data in staging area locally 
 3. Ingesting the staged data into BigQuery from the GCS bucket. 
### Visual Representation
IMAGE GOES HERE
### Scraping URL for links
I initially wanted to use the requests library or the bs4 library to scrape the URLs, however I realized that 
the webpage was bundled using webpack, so I decided to use Selenium instead to load the webpage, and extract the necessary links 
from the generated the XML content. I manually inspected the elements of the links under the 'Buisiness' Header element
### Dowloading and Uploaing links into a DataStore : GCS
Initially I was staging files on my local machine in the tmp/ directory, however I realized that this was a sub-optimal solution, given that 
I was writing a file locally to be uploaded anyway. I decided instead to use an IO steam,  writing the content of the dowload url to a file buffer, and uploaing that content directly to GCS, where it could be officially 'stored'. I made this decision as given any potential issues with the table in BigQuery, than the file can be inspected manually from GCS. 
### Ingesting Links to BigQuery from GCS
I initally was storing the downloaded gcs links as a compressed .gz file, as I had read up that BigQuery is automatically configured to handle the .gz file format for ingestion. However, I realized that some of the links were not only .zip or .csv files but also .xlsx files, and that some of the schemas for certain tables could not be accurately infered by the BigQuery.JobConfig() class. As such I needed to implement two ways to handle these exceptions. Handling expections was done by downloading the dataset from GCS, either converting the file to a .csv format and replacing the existing file in GCS or manually gernerating the table schema for the BigQuery Loader. 
## Run Program
### Install
```bash
git clone https://github.com/lukaselsrode/dag_airflow_assignment.git
cd dag_airflow_assignment
```
#### Setup Environment & Dependencies
```bash
python3 -m venv .env/ 
source .env/bin/activate
pip install --upgrade pip  # airflow dependencies can be annoying
pip install -r requirements.txt
```
#### Selenium Chrome Driver Requirement
this script uses the Selenium library and the Chrome driver, make sure that you have installed and configured the Chrom webdriver: https://sites.google.com/chromium.org/driver/
#### Google Service Account Secret JSON file 
The execution of the script requires acces to the Google Cloud nz_business project, create a service account for the project and download the secret JSON file
for the service account place it somewhere in your project directory and alter this line of code in /dag/nz_buisiness_pipeline.py to refrence that secret file. 
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
#### From Airflow
I messed around with configuring the docker-compose.yml file, and tried setting up a webserver to test this but ultimately found that the airflowctl project was a good resource for testing this project locally, although note that the virtual environment will need to be re-configured for testing the code this way. 
https://github.com/kaxil/airflowctl