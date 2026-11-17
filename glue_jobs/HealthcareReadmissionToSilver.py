import json
import boto3
import sys

from awsglue.utils import getResolvedOptions

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lpad,
    length,
    trim,
    to_date,
    when
)
from pyspark.sql.types import DoubleType

from delta.tables import DeltaTable


# --------------------------------------------------
# 1. Read Glue job parameters
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["bucket_name", "manifest_key"]
)

bucket_name = args["bucket_name"]
manifest_key = args["manifest_key"]


# --------------------------------------------------
# 2. Read the manifest from S3
# --------------------------------------------------

s3 = boto3.client("s3")

response = s3.get_object(
    Bucket=bucket_name,
    Key=manifest_key
)

manifest_content = response["Body"].read().decode("utf-8")
manifest = json.loads(manifest_content)

files_to_process = manifest.get("files", [])

# This job processes readmission entries only.
readmission_files = [
    file for file in files_to_process
    if file["dataset_type"] == "readmission_performance"
]

if len(readmission_files) > 1:
    raise ValueError(
        "Multiple readmission snapshots found in the manifest. "
        "Resolve which snapshot is authoritative before processing."
    )

print(f"Readmission files awaiting processing: {len(readmission_files)}")

if not readmission_files:
    print("No readmission files to process. Job completed.")

# --------------------------------------------------
# 3. Initialize Spark session and Delta configuration
# --------------------------------------------------

spark = SparkSession.builder.getOrCreate()

# Enable Change Data Feed by default for new Delta tables.
spark.conf.set(
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
    "true"
)


# --------------------------------------------------
# 4. Define helper function to read Bronze CSV files
# --------------------------------------------------

def read_bronze_csv(s3_key):

    source_path = f"s3://{bucket_name}/{s3_key}"

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )


# --------------------------------------------------
# 5. Standardize CCN
# --------------------------------------------------

def standardize_ccn(df):

    df = df.withColumn(
        "CCN",
        when(col("CCN").isNull(), None)
        .otherwise(trim(col("CCN")))
    )

    df = df.withColumn(
        "CCN",
        when(
            length(col("CCN")) < 6,
            lpad(col("CCN"), 6, "0")
        )
        .otherwise(col("CCN"))
    )

    return df


# --------------------------------------------------
# 6. Transform readmission data
# --------------------------------------------------

def transform_readmission(df):
    #1. Retain required fields and use a common identifier name
    df = df.select(
        col("CMS Certification Number (CCN)").alias("CCN"),
        col("Provider Name").alias("provider_name"),
        col("State").alias("state"),
        col("Performance Period: FY 2022 Risk-Standardized Readmission Rate").alias("fy_2022_risk_standardized_readmission_rate")
    )

    #2. Standardize CCN as a six-character string
    df = standardize_ccn(df)

    #3. Cast readmission rate to DoubleType
    df = df.withColumn(
        "fy_2022_risk_standardized_readmission_rate",
        col("fy_2022_risk_standardized_readmission_rate").cast(DoubleType())
    )

    return df

# --------------------------------------------------
# 7. Validate transformed readmission data
# --------------------------------------------------

def validate_before_write(
    df,
    dataset_type,
    required_columns,
    key_columns
):

    # 1. Check required columns.
    for column_name in required_columns:

        if column_name not in df.columns:
            raise ValueError(
                f"{dataset_type}: Required column is missing: "
                f"{column_name}"
            )

    # 2. Check for an empty dataset.
    row_count = df.count()

    if row_count == 0:
        raise ValueError(
            f"{dataset_type}: Dataset is empty."
        )

    # 3. Check for null values in key columns.
    for column_name in key_columns:

        null_count = df.filter(
            col(column_name).isNull()
        ).count()

        if null_count > 0:
            raise ValueError(
                f"{dataset_type}: {null_count} records have "
                f"null values in {column_name}."
            )

    # 4. Check CCN format.
    invalid_ccn_count = df.filter(
        col("CCN").isNull()
        | ~col("CCN").rlike("^[A-Za-z0-9]{6}$")
    ).count()

    if invalid_ccn_count > 0:
        raise ValueError(
            f"{dataset_type}: {invalid_ccn_count} CCNs "
            "are not six-character alphanumeric identifiers."
        )

    # 5. Check for duplicate keys.
    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{dataset_type}: {duplicate_count} duplicate "
            "key groups found."
        )

    print(
        f"VALIDATION PASSED: {dataset_type} | Rows: {row_count}"
    )

# --------------------------------------------------
# 8. Validate data after writing to Silver
# --------------------------------------------------

def validate_after_write(
    silver_path,
    dataset_type,
    expected_row_count,
    required_columns,
    key_columns
):

    # 1. Read the persisted Delta table from Silver.
    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # 2. Check required columns.
    for column_name in required_columns:

        if column_name not in silver_df.columns:
            raise ValueError(
                f"{dataset_type}: Post-write validation failed. "
                f"Missing column: {column_name}"
            )

    # 3. Compare Silver row count with incoming snapshot.
    actual_row_count = silver_df.count()

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"{dataset_type}: Post-write row count mismatch. "
            f"Expected: {expected_row_count}, "
            f"Actual: {actual_row_count}"
        )

    # 4. Check for null values in key columns.
    for column_name in key_columns:

        null_count = silver_df.filter(
            col(column_name).isNull()
        ).count()

        if null_count > 0:
            raise ValueError(
                f"{dataset_type}: Post-write validation failed. "
                f"{null_count} null values found in {column_name}."
            )

    # 5. Check for duplicate keys.
    duplicate_count = (
        silver_df.groupBy(*key_columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{dataset_type}: Post-write validation failed. "
            f"{duplicate_count} duplicate key groups found."
        )

    print(
        f"POST-WRITE VALIDATION PASSED: {dataset_type} | "
        f"Rows: {actual_row_count} | "
        f"Keys: {key_columns}"
    )
    
# --------------------------------------------------
# 9. Define readmission validation requirements
# --------------------------------------------------

required_columns = [
    "CCN",
    "provider_name",
    "state",
    "fy_2022_risk_standardized_readmission_rate"
]

key_columns = ["CCN"]


# --------------------------------------------------
# 10. Define the Silver MERGE function
# --------------------------------------------------

def merge_into_silver(
    df,
    silver_path,
    key_columns,
    dataset_type,
    required_columns
):

    # Capture the expected row count from the incoming snapshot.
    expected_row_count = df.count()

    # 1. Check whether the Silver Delta table already exists.
    if DeltaTable.isDeltaTable(spark, silver_path):

        target = DeltaTable.forPath(spark, silver_path)

        # 2. Build the matching condition.
        merge_condition = " AND ".join(
            [
                f"target.`{key}` = source.`{key}`"
                for key in key_columns
            ]
        )

        # 3. Match incoming records against Silver.
        merge = target.alias("target").merge(
            df.alias("source"),
            merge_condition
        )

        # 4. Update existing records.
        merge = merge.whenMatchedUpdateAll(
            condition="""
                NOT (
                    target.provider_name <=> source.provider_name
                    AND target.state <=> source.state
                    AND target.fy_2022_risk_standardized_readmission_rate <=> source.fy_2022_risk_standardized_readmission_rate
                )
            """
        )

        # 5. Insert new records.
        merge = merge.whenNotMatchedInsertAll()

        # 6. Delete records missing from the new snapshot.
        merge = merge.whenNotMatchedBySourceDelete()

        # 7. Execute the MERGE.
        merge.execute()

    else:

        # Create the Silver Delta table on its first load.
        df.write.format("delta").mode("overwrite").save(silver_path)

    print(f"SILVER WRITE SUCCESSFUL: {silver_path}")

    # 8. Validate the persisted Silver table.
    validate_after_write(
        silver_path,
        dataset_type,
        expected_row_count,
        required_columns,
        key_columns
    )

# --------------------------------------------------
# 11. Process readmission files
# --------------------------------------------------

for file in readmission_files:

    dataset_type = file["dataset_type"]
    bronze_s3_key = file["bronze_s3_key"]
    target_silver_table = file["target_silver_table"]

    # Guard against an incorrectly configured manifest entry.
    if target_silver_table != "readmission_performance":
        raise ValueError(
            f"Unexpected Silver target for readmission job: "
            f"{target_silver_table}"
        )

    print(f"Starting Silver processing: {dataset_type}")

    # 1. Read the Bronze CSV.
    df = read_bronze_csv(bronze_s3_key)

    # 2. Apply readmission transformations.
    transformed_df = transform_readmission(df)

    # 3. Validate the transformed DataFrame.
    validate_before_write(
        transformed_df,
        dataset_type,
        required_columns,
        key_columns
    )

    # 4. Determine the Silver Delta table location.
    silver_path = (
        f"s3://{bucket_name}/silver/{target_silver_table}/"
    )

    # 5. Merge the validated snapshot into Silver.
    merge_into_silver(
        transformed_df,
        silver_path,
        key_columns,
        dataset_type,
        required_columns
    )

print("READMISSION SILVER JOB COMPLETED SUCCESSFULLY.")