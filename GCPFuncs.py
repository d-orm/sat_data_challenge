from google.cloud import storage
import gspread
from gspread_dataframe import set_with_dataframe
import os
from datetime import datetime
import pandas as pd
from meteostat import Point

# Variable for weather API
london = Point(51.504, -0.129, 70)

# Variables for GCP
# https://console.cloud.google.com/bigquery?project=sat-data-engineer-challenge
# service@sat-data-engineer-challenge.iam.gserviceaccount.com
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'sat-data-engineer-challenge-90ebb4111507.json'
client = storage.Client()
bucket = client.get_bucket('visits_data_bucket')
BQ_PROJECT, BQ_DATASET = "sat-data-engineer-challenge", "sat_data"
BQ_WEATHER = f"{BQ_DATASET}.weather"
BQ_VISITS = f"{BQ_DATASET}.visits"
BQ_NETWORK_NODES = f"{BQ_DATASET}.network_nodes"
BQ_NETWORK_EDGES = f"{BQ_DATASET}.network_edges"
BQ_NETWORK_MATRIX = f"{BQ_DATASET}.network_matrix"
BQ_NETWORK_HOPS = f"{BQ_DATASET}.network_hops"
BQ_NETWORK_CL = f"{BQ_DATASET}.network_clusters"
# Url for Google Sheets visits log database
visits_log = "https://docs.google.com/spreadsheets/d/1i5hQao6sZG5CMhGuqNE1xOOgg5s3KVLKpihQp4suBn4/edit#gid=0"
# Variables for initial file names
network_file = "network.adjlist"
visits_file = "visits.txt"


# Big Query/GCP bucket functions
def upload_to_bq_table(df, table):
    """ Uploads dataframes to Big Query tables """
    print("Uploading to BQ table...")
    df.to_gbq(table, BQ_PROJECT, if_exists='append')
    print("Success")


def list_bucket_contents(buck):
    """ Returns a list of files names in a GCP bucket """
    files = []
    for blob in client.list_blobs(buck):
        files.append(str(blob).split(", ")[1])

    return files


def dl_from_gcp_bucket(buck, file):
    """ Downloads a file from a GCP bucket, also returns file path as string """
    timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    blob = buck.blob(file)
    file_path = f'{timestamp}_{file}'
    blob.download_to_filename(file_path)

    return file_path


# Google Sheets functions
def connect_to_sheet(url):
    """ Authenticates and connects to a Google Sheet """
    url = url.split("spreadsheets/d/")
    wb_id = url[1].split("/edit#gid=")[0]
    gc = gspread.service_account(filename='sat-data-engineer-challenge-90ebb4111507.json')
    sh = gc.open_by_key(f'{wb_id}')
    return sh


def get_df_from_g_sheet(url, sheet_index):
    """ Reads values from a google sheet to a dataframe """
    sh = connect_to_sheet(url)
    df = pd.DataFrame(sh.worksheet(sheet_index).get_all_records())
    return df


def write_df_to_g_sheet(url, sheet_index, df, headers=False):
    """ Writes a dataframe to a Google Sheet """
    sh = connect_to_sheet(url)
    worksheet = sh.get_worksheet(sheet_index)  # 0=first sheet, 1=second sheet, etc
    print("Writing to Google Sheet...")
    # Append DataFrame to bottom row
    set_with_dataframe(worksheet, df, row=len(worksheet.get_all_values()) + 1, include_column_header=headers)
    # Append blank row to bottom (for next import)
    worksheet.append_row([''])
    print("Success")
