import ast
import re
from typing import Dict

import pandas as pd
from google.cloud import bigquery


def load_ingestion_data_as_DataFrame(config: Dict) -> pd.DataFrame:
    """Extract data form big query database and convert it to pd.DataFrame.

    Depending on the ´target_garage´, it connects to the corresponding BQ query.
    Args:
        config: glass evaluation config

    Returns:
        ingestion DataFrame
    """
    config_ingestion = config["data_ingestion"]
    project_id = config_ingestion["project_id"]
    region = config_ingestion["region"]
    target_garage = config["dataset_info"]["target_garage"]
    BQ_query_name = config_ingestion[f"BQ_query_name_{target_garage}"]
    print(BQ_query_name)
    client = bigquery.Client(project=project_id, location=region)

    QUERY = f""" SELECT * FROM
    {BQ_query_name}"""
    query_job = client.query(QUERY)  # API request
    ingestion_df = query_job.to_dataframe()

    # this part of code is added as by using <to_dataframe>
    # the boolean True converted to true which is not a defined
    # value in python. this cause an error later where ast.lietral_values()
    # were used.

    for idx, row in ingestion_df.iterrows():
        if row["imageToLabelData"]:
            ingestion_df.at[idx, "imageToLabelData"] = _convert_replace_true_false(
                row["imageToLabelData"]
            )

    if (config["dataset_info"]["target_garage"] == "pilot_garages") or (
        config["dataset_info"]["target_garage"] == "C2C-Garage"
    ):
        for idx, row in ingestion_df.iterrows():
            if row["garageAction"]:
                ingestion_df.at[
                    idx, "garageAction"
                ] = _convert_stringified_json_to_dict(row["garageAction"])

    return ingestion_df


def _convert_replace_true_false(input_str: str) -> str:
    """Helper function to convert an stringified json to dict.

    This function is needed as due to using to_datframe() true/false in json
    format would appear in pd.datframe which are not readable by python.
    Here first we replace true with True and false with False, then we convert
    the string input to a dict.

    Args:
        input_str: stringified json

    Returns:
        original dict
    """
    if "true" in input_str:
        input_str = re.sub("true", "True", input_str)
    if "false" in input_str:
        input_str = re.sub("false", "False", input_str)

    return input_str


def _convert_stringified_json_to_dict(input_str: str) -> Dict:
    """Helper function to convert an stringified json to dict.

    This function is needed as due to using to_datframe() true/false in json
    format would appear in pd.datframe which are not readable by python.
    Here first we replace true with True and false with False, then we convert
    the string input to a dict.

    Args:
        input_str: stringified json

    Returns:
        original dict
    """
    if "true" in input_str:
        input_str = re.sub("true", "True", input_str)
    if "false" in input_str:
        input_str = re.sub("false", "False", input_str)

    if "null" in input_str:
        input_str = re.sub("null", "None", input_str)

    # convert to dict
    output_dict = ast.literal_eval(input_str)

    return output_dict
