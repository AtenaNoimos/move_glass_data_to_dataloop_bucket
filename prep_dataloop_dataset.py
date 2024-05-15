# from fastapi import HTTPException
from google.cloud import exceptions
from requests.exceptions import ConnectionError as RequestsConnectionError
from urllib3.exceptions import HTTPError

from notebooks.dataloop_data_prep.data_ingestion import load_ingestion_data_as_DataFrame
from notebooks.dataloop_data_prep.preprocessing_utils import (
    filter_for_data_type,
    preprocess_claim_df_dataloop,
)
from notebooks.dataloop_data_prep.utils import (
    copy_blob_between_two_gcs_projects,
    filter_ingestion_df_for_specific_period,
)
from src.packages.noimos_utils.noimos.utils.gcp_functions import blob_exists
from src.packages.noimos_utils.noimos.utils.general import read_yaml


def prep_dataloop_dataset(
    config_path: str,
    createdAt_lower_limit: str = None,
    createdAt_upper_limit: str = None,
):
    config = read_yaml(config_path)
    target_garage = config["dataset_info"]["target_garage"]

    source_project_name = config["bucket_info"]["source_project_name"]
    source_bucket_name = config["bucket_info"]["source_bucket_name"]
    destination_project_name = config["bucket_info"]["destination_project_name"]
    destination_bucket_name = config["bucket_info"]["destination_bucket_name"]

    ingestion_df = load_ingestion_data_as_DataFrame(config)
    ingestion_df = preprocess_claim_df_dataloop(ingestion_df, config["preprocessing"])

    ingestion_df = filter_for_data_type(config, ingestion_df)

    ingestion_df = filter_ingestion_df_for_specific_period(
        ingestion_df, createdAt_lower_limit, createdAt_upper_limit
    )
    destination_folder_prefix = config["dataset_info"]["dataloop_dataset_folder_prefix"]
    data_type = config["dataset_info"]["data_type"]

    print(
        f"Total number of {data_type} images for {target_garage}: ", len(ingestion_df)
    )

    destination_folder_name = (
        f"{destination_folder_prefix}{target_garage}_{data_type}_2"
    )
    cnt_new_cases = 0

    for _, row in ingestion_df.iterrows():
        claim_id = row["claimId"]
        file_name = row["storageURI"]
        source_blob_name = f"{claim_id}/{file_name}"
        destination_blob_name = f"{destination_folder_name}/{file_name}"

        if blob_exists(
            destination_bucket_name, destination_project_name, destination_blob_name
        ):
            print("Image file already exists, skip coping file...")
        else:
            try:
                copy_blob_between_two_gcs_projects(
                    source_project_name,
                    source_bucket_name,
                    source_blob_name,
                    destination_project_name,
                    destination_bucket_name,
                    destination_blob_name,
                )
                cnt_new_cases += 1
            except exceptions.NotFound as err:
                # if err.code == 404:
                print("Image does not exist in source bucket", err)
                print("source_blob_name", source_blob_name)
                print("destination_blob_name", destination_blob_name)
            #     pass
            # else:
            #     raise Exception("Sorry other errors")
            except Exception as err:
                print("Connection error", err)
                print("source_blob_name", source_blob_name)
                print("destination_blob_name", destination_blob_name)
    print("Number of new cases:", cnt_new_cases)

    return ingestion_df
