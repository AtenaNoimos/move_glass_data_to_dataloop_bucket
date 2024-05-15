from typing import List

import pandas as pd

from src.glass_pipeline.glass_pipeline.utils.preprocessing_utils import (
    _convert_column_types,
    _map_image_type_columns,
)


def _filter_ingestion_df(
    claim_df: pd.DataFrame, columns_to_keep: List[str]
) -> pd.DataFrame:
    """Filter ingestion df to only keep relevant images.

    In particular, we filter out images that are deleted, not windshield
    or detail images, and images with duplicate imageIds.

    This function assumes that column types have been converted to the
    correct types, especially the deletedAt column should be of type
    datetime.

    Args:
        claim_df: Ingestion dataframe
        preprocessing_config: Pre-processing config

    Returns:
        Filtered ingestion dataframe
    """

    claim_df = claim_df.copy(deep=True)

    claim_df = claim_df[claim_df["deletedAt"].isna()]

    claim_df = claim_df[claim_df["type"].isin(["windshield", "detail"])]

    claim_df = claim_df.drop_duplicates(subset=["imageId"], keep="first")

    # Only keep necessary columns for pipeline
    claim_df = claim_df[columns_to_keep]

    return claim_df


def preprocess_claim_df_dataloop(
    claim_df: pd.DataFrame, preprocessing_config: dict
) -> pd.DataFrame:
    closeup_exclude_types_list = preprocessing_config["image_type_mapping"][
        "closeup_exclude_types"
    ]

    closeup_type_list = (
        claim_df[~claim_df.type.isin(closeup_exclude_types_list)]["type"]
        .unique()
        .tolist()
    )

    closeup_type_mapping = {
        closeup_type_list[i]: "detail" for i in range(len(closeup_type_list))
    }
    print(closeup_type_list)

    image_type_mapping = {
        **preprocessing_config["image_type_mapping"]["fullview_include_types_mapping"],
        **closeup_type_mapping,
    }

    claim_df = _map_image_type_columns(
        claim_df=claim_df, image_type_mapping=image_type_mapping
    )

    claim_df = _convert_column_types(
        claim_df=claim_df,
        column_type_mapping=preprocessing_config["column_type_mapping"],
    )

    claim_df = _filter_ingestion_df(
        claim_df=claim_df, columns_to_keep=preprocessing_config["columns_to_keep"]
    )
    return claim_df


def filter_for_data_type(config, ingestion_df):
    data_type = config["dataset_info"]["data_type"]

    return ingestion_df[ingestion_df["type"] == data_type]
