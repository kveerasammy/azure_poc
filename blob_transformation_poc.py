from azure.storage.blob import BlobServiceClient
from io import StringIO 
import pandas as pd
from datetime import datetime
import os 

account_url = os.environ.get("AZURE_ACCOUNT_URL")
shared_access_key = os.environ.get("AZURE_ACCESS_KEY")

blob_service_client = BlobServiceClient(account_url=account_url,
                                        credential=shared_access_key)

def list_blobs_in_container(container_name="landing-zone"):
    """
    This function will list blobs in a container
    """
    container = blob_service_client.get_container_client(container=container_name)
    blob_list = container.list_blobs()
    source_blobs = []
    for blob in blob_list:
        source_blobs.append(blob['name'])
    return source_blobs

def read_blob_into_df_and_transform(source_blobs: list, container_name:str) -> pd.DataFrame:
    """
    This function will read blobs into a pandas dataframe
    """
    try:
        print("reading blob.. transforming")
        dfs = []
        for blob in source_blobs:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
            blob_content = blob_client.download_blob().readall()
            csv_data = StringIO(blob_content.decode('utf-8'))
            df = pd.read_csv(csv_data)
            dfs.append(df)
        df_main = pd.concat(dfs)
        df_main['timestamp'] = pd.to_datetime(datetime.now())
        print(f"Data -{df_main.head()}")
        return df_main
    except Exception as e:
        print(f"An exception was raised in read_blob_into_df_and_transform() - {e}")
        os._exit(1)

def upload_transformed_df_to_tz(df_transformed, tz_container: str):
    """
    This function will upload the transformed pandas df to azure
    """
    try:
        path_to_csv_file = os.path.join(os.getcwd(), "extracts", "Customers_withTimestamp.csv")
        df_transformed.to_csv(path_to_csv_file ,sep=",", index=False)
        print(f"Saved Transformed CSV File to {path_to_csv_file}")
        container_client = blob_service_client.get_container_client(container=tz_container)
        with open(file=path_to_csv_file, mode="rb") as data:
            blob_client = container_client.upload_blob(name="final/Customers_withTimestamp.csv", data=data, overwrite=True)
            print("Done")
    except Exception as e:
        print(f"An exception was raised in upload_transformed_df_to_tz() - {e}")
        os._exit(1)

def main():
    source_blobs = list_blobs_in_container()
    df_main = read_blob_into_df_and_transform(source_blobs=source_blobs, container_name="landing-zone")
    upload_transformed_df_to_tz(df_transformed=df_main, tz_container="trusted-zone")


main()