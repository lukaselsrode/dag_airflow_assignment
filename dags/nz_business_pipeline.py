import os
import io
import zipfile
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage, bigquery
from selenium import webdriver
from selenium.webdriver.common.by import By

# note config, could be arg.parse or .yaml config file but just trying to limit files uploaded
## SCRIPT CONFIG  #####################################################################
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../.secrete.json" # path to your google service account credentials
EXTERNAL_URL = "https://www.stats.govt.nz/large-datasets/csv-files-for-download" # url to scrape 
GCS_BUCKET = "nz_business_bucket" # name of the GCS bucket to download the links to / staging area
BQ_DATASET = "nz_business"        # 


## AUXILERY FUNCTIONS #################################################################
def execute_in_thread_pool(function, args_list: list[tuple]) -> None:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(function, *args) for args in args_list]
        for future in as_completed(futures):
            future.result()

def fmt_args(args:list[any]) -> list[tuple]:
    return [(a,) for a in args]


def get_request(link: str):
    return requests.get(link)


## MAIN FUNCTIONS ######################################################################
def get_links() -> list[str]:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    browser = webdriver.Chrome(options=options)
    browser.get(EXTERNAL_URL)
    download_links = browser.find_elements(
        By.XPATH,
        "//h2[contains(text(), 'Business')]/ancestor::*[contains(.//a[@download], '')][1]//a[@download]",
    )
    links = list(map(lambda l: l.get_attribute("href"),download_links))
    browser.quit()
    ulinks = list(
        set(links)
    )  # no duplicates... scraping could be better, but this is a hacky fix
    print(f"Found {len(ulinks)} links on {EXTERNAL_URL} \n")
    return ulinks


def stage_files_in_gcs(links: list[str] = get_links()) -> None:
    print(f'Staging files in GCS Bucket {GCS_BUCKET} ...')
    def setup_store():
        global storage_client
        global bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)

    def upload_to_gcs(file_object, destination_blob_name):
        try:
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_file(file_object)
        except Exception as e:
            print(f"Failed to stage {destination_blob_name} due to {e}")

    def handle_zip_file(response):
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            for zip_info in zip_ref.infolist():
                if not zip_info.is_dir():
                    with zip_ref.open(zip_info) as f, io.BytesIO() as file_buffer:
                        file_buffer.write(f.read())
                        file_buffer.seek(0)
                        upload_to_gcs(file_buffer, zip_info.filename)

    def upload_base_file(response, filename):
        with io.BytesIO(response.content) as file_buffer:
            upload_to_gcs(file_buffer, filename)

    def upload_zip(link):
        handle_zip_file(get_request(link))

    def upload_file(link):
        upload_base_file(get_request(link), os.path.basename(link))

    def download_and_upload_link(link: str):
        upload_zip(link) if link.endswith(".zip") else upload_file(link)

    setup_store()
    execute_in_thread_pool(download_and_upload_link, fmt_args(links))
    print('Files Staged successfully! \n')


def ingest_data_from_storage_to_bigquery():
    print(f'Ingesting Data from storage to bigquery: {GCS_BUCKET} -> {BQ_DATASET}')
    client = bigquery.Client()
    dataset_ref = client.dataset(BQ_DATASET)
    # this is a workaround, not elegant but it works.
    def install_tmp_df(blob, is_csv=True):
        tmp_loc = f"/tmp/temp_{blob.name}"
        blob.download_to_filename(tmp_loc)
        return tmp_loc, pd.read_csv(tmp_loc) if is_csv else pd.read_excel(tmp_loc)

    # hacky but it works...
    def generate_bq_schema(dataframe):
        type_mapping = {
            "object": "STRING",
            "int64": "INTEGER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
        }
        schema = []
        for column_name, dtype in dataframe.dtypes.items():
            bq_type = type_mapping.get(
                str(dtype), "STRING"
            )  # Default to STRING, seems to be the case with 'code' fields
            schema.append(bigquery.SchemaField(column_name, bq_type))
        return schema

    # very hacky, ideally I'd use a parser to generate a csv but this is faster...
    def convert_xlsx_to_csv(blob):
        tmp_loc, df = install_tmp_df(blob, is_csv=False)
        csv_path = "/tmp/temp_converted.csv"
        df.to_csv(csv_path, index=False)
        new_blob_name = blob.name.replace(".xlsx", ".csv")
        bucket.blob(new_blob_name).upload_from_filename(csv_path)
        os.remove(tmp_loc),os.remove(csv_path)
        return bucket.blob(new_blob_name)

    def auto_ingest_from_gcs(blob, table_ref):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            allow_quoted_newlines=True,
        )
        load_job = client.load_table_from_uri(
            f"gs://{GCS_BUCKET}/{blob.name}", table_ref, job_config=job_config
        )
        load_job.result()

    def ingest_after_infer_scheme_from_tmp_df(blob, table_ref):
        location, tmp_df = install_tmp_df(blob)
        schema = generate_bq_schema(tmp_df)
        table_ref = dataset_ref.table(blob.name.split(".")[0])
        client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)
        load_job = client.load_table_from_uri(
            f"gs://{GCS_BUCKET}/{blob.name}",
            table_ref,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
            ),
        )
        load_job.result()
        os.remove(location)

    def ingest_blob(blob):
        if blob.name.endswith(".xlsx"): blob = convert_xlsx_to_csv(blob)
        table_ref = dataset_ref.table(blob.name.split(".")[0])
        try:
            auto_ingest_from_gcs(blob, table_ref)
        except Exception:
            ingest_after_infer_scheme_from_tmp_df(blob, table_ref)

    execute_in_thread_pool(ingest_blob,fmt_args(bucket.list_blobs()))
    print('ALL BQUERY DATA INGESTED!')

"""

def main():
    stage_files_in_gcs()
    ingest_data_from_storage_to_bigquery()

if __name__ == "__main__":
    main()
"""
