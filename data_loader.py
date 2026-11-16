
import boto3
import streamlit as st
from deltalake import DeltaTable


# 1. Define the S3 bucket and AWS configuration
BUCKET_NAME = "mb-healthcare-metrics"
AWS_REGION = st.secrets["AWS_DEFAULT_REGION"]


# 2. Create a function to establish the AWS session
def get_storage_options():

    session = boto3.Session(
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=AWS_REGION
    )

    credentials = session.get_credentials().get_frozen_credentials()

    storage_options = {
        "AWS_ACCESS_KEY_ID": credentials.access_key,
        "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
        "AWS_REGION": AWS_REGION
    }

    if credentials.token:
        storage_options["AWS_SESSION_TOKEN"] = credentials.token

    return storage_options


# 3. Create a reusable function to load a Gold Delta table
@st.cache_data(ttl=3600)
def load_gold_table(table_name):

    storage_options = get_storage_options()

    table_path = (
        f"s3://{BUCKET_NAME}/gold/{table_name}/"
    )

    delta_table = DeltaTable(
        table_path,
        storage_options=storage_options
    )

    dataframe = delta_table.to_pandas()

    return dataframe
