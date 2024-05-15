"""Utility functions for glass evaluation pipeline."""

import datetime
import time
from typing import Any, Dict, List

import pandas as pd
from google.cloud import storage


def validate_input_time_format(input_string: str) -> Any:
    """Validate the date and time format.

    Args:
        input_string (str): input string

    Raises:
        ValueError: Raise value error if the format is not correct
    """
    validate_date(input_string[0:10])
    if input_string[10] != " ":
        raise ValueError("Incorrect data format, missing space")
    validate_hours(input_string[11:19])


def validate_date(date_text) -> Any:
    """Validate date.

    Args:
        date_text: input date text

    Raises:
        ValueError: Raise value error if date format is not correct
    """
    try:
        datetime.date.fromisoformat(date_text)
    except ValueError as e:
        raise ValueError(f"Incorrect data format, should be YYYY-MM-DD: {e}")


def validate_hours(time_text) -> Any:
    """Validate time format.

    Args:
        time_text: input time text

    Raises:
        ValueError: Raise value error if time format is not correct
    """
    try:
        time.strptime(time_text, "%H:%M:%S")
    except ValueError as e:
        raise ValueError(f"Incorrect data format, should be H:M:S {e}")


def filter_ingestion_df_for_specific_period(
    ingestion_df: pd.DataFrame, createdAt_lower_limit: str, createdAt_upper_limit: str
) -> pd.DataFrame:
    """Filter claims that are created in a specific time period.

    Args:
        ingestion_df: Ingestion DataFrame
        createdAt_lower_limit: Lower_limit for the claims creation time.
        createdAt_upper_limit (str): Upper_limit for the claims creation time.

    Returns:
        filtered data.
    """

    if not createdAt_lower_limit and createdAt_upper_limit:
        validate_input_time_format(createdAt_upper_limit)
        ingestion_df = ingestion_df[ingestion_df["createdAt"] <= createdAt_upper_limit]
    elif createdAt_lower_limit and not createdAt_upper_limit:
        validate_input_time_format(createdAt_lower_limit)
        ingestion_df = ingestion_df[ingestion_df["createdAt"] >= createdAt_lower_limit]
    elif createdAt_lower_limit and createdAt_upper_limit:
        validate_input_time_format(createdAt_lower_limit)
        validate_input_time_format(createdAt_upper_limit)
        ingestion_df = ingestion_df[
            ingestion_df["createdAt"].between(
                createdAt_lower_limit, createdAt_upper_limit
            )
        ]

    return ingestion_df


def validate_target_garage_name(input_text: str):
    """Validate `target_garage` in the config."""
    if input_text == "pilot_garages":
        pass
    elif input_text == "desa":
        pass
    elif input_text == "car_glass":
        pass
    elif input_text == "sams_autoglass":
        pass
    elif input_text == "C2C-Garage":
        pass
    elif input_text == "C2C-Customer":
        pass
    elif input_text == "Axa_France":
        pass
    else:
        raise ValueError("`target_garage` is not valid!")


def copy_blob_between_two_gcs_projects(
    source_project_name,
    source_bucket_name,
    source_blob_name,
    destination_project_name,
    destination_bucket_name,
    destination_blob_name=None,
):
    """Copies a blob from one bucket to another one in a different gcs project ."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    source_storage_client = storage.Client(project=source_project_name)
    destination_storage_client = storage.Client(project=destination_project_name)

    source_bucket = source_storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = destination_storage_client.bucket(destination_bucket_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to copy is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # There is also an `if_source_generation_match` parameter, which is not used in this example.
    destination_generation_match_precondition = 0

    if destination_blob_name:
        blob_copy = source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            destination_blob_name,
            if_generation_match=destination_generation_match_precondition,
        )
    else:
        blob_copy = source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            source_blob_name,
            if_generation_match=destination_generation_match_precondition,
        )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )
