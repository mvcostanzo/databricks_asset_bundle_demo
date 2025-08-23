from typing import Any, Optional

import dlt
from dlt.destinations import databricks
from dlt.sources.rest_api.typing import RESTAPIConfig, OffsetPaginatorConfig
from dlt.sources.rest_api.utils import check_connection
from dlt.sources.rest_api import (
    rest_api_resources,
    rest_api_source,
)
import os
import boto3

# Disable AWS credential chain that tries to use STS
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

session = boto3.Session(
    aws_access_key_id=dlt.secrets['destination.filesystem.credentials.aws_access_key_id'],
    aws_secret_access_key=dlt.secrets['destination.filesystem.credentials.aws_secret_access_key'],
    region_name='wnam'
)

boto3.setup_default_session(
    aws_access_key_id=dlt.secrets['destination.filesystem.credentials.aws_access_key_id'],
    aws_secret_access_key=dlt.secrets['destination.filesystem.credentials.aws_secret_access_key'],
    region_name='wnam'
)


def build_cdc_config() -> RESTAPIConfig:
    paginator: OffsetPaginatorConfig = {
        "type": "offset",
        "limit": 1000,
        "limit_param": "$limit",
        "offset_param": "$offset",
        "stop_after_empty_page":True,
        "maximum_offset": 100000000,
        "total_path": None
    }
    config: RESTAPIConfig = {
        "client":{
            "base_url": "https://data.cdc.gov/resource",
            "paginator": paginator,
        },
        "resources":[
            {
                "name": "nndss",
                "endpoint":{
                    "method": "GET",
                    "path": "x9gk-5huc.json",
                    "params":{
                        "sort_order": "{incremental.initial_value}"
                    },
                    "incremental": {
                        "cursor_path": "sort_order",
                        'initial_value': '20220100001'
                    },                       
                },
                "primary_key": "sort_order",
                "file_format": "parquet"
            },
        ]
    }
    return config

@dlt.source(name="nndss")
def cdc_data_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
    cdc_data_config = build_cdc_config()
    yield from rest_api_resources(cdc_data_config)


def load_cdc_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="load_cdc_data",
        destination='databricks',
        dataset_name="nndss",
        progress="enlighten",
        pipelines_dir='./dlt_pipelines',
        staging='filesystem'
    )

    can_connect, error_msg = check_connection(cdc_data_source(), "nndss")
    if can_connect:
        load_info = pipeline.run(cdc_data_source)
        print(load_info)  # noqa: T201

if __name__ == "__main__":
    load_cdc_data()