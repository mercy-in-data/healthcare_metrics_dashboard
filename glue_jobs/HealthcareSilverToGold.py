import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, lit
)
from delta.tables import DeltaTable

# --------------------------------------------------
# 1. Retrieve job parameters
# --------------------------------------------------
args = getResolvedOptions(sys.argv, ["bucket_name"])
bucket_name = args["bucket_name"]

# --------------------------------------------------
# 2. Initialize spark session and configuration
# --------------------------------------------------

# Create Spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# --------------------------------------------------
# 3. Retrieve Silver Delta versions and read pinned snapshots
# --------------------------------------------------
# Define Silver table paths
staffing_path = f"s3://{bucket_name}/silver/staffing/"
provider_info_path = f"s3://{bucket_name}/silver/provider_info/"
readmission_path = f"s3://{bucket_name}/silver/readmission_performance/"

# Retrieve the latest committed Delta version
def get_latest_delta_version(table_path, table_name):

    history_df = DeltaTable.forPath(
        spark, table_path
    ).history(1)

    version = history_df.select("version").first()["version"]

    print(f"{table_name} latest Delta version: {version}")

    return version


# Capture the latest version of each Silver table
staffing_version = get_latest_delta_version(
    staffing_path, "Staffing"
)

provider_info_version = get_latest_delta_version(
    provider_info_path, "Provider Info"
)

readmission_version = get_latest_delta_version(
    readmission_path, "Readmission Performance"
)

# Read each Silver table using its captured version
staffing_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", staffing_version)
    .load(staffing_path)
)

provider_info_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", provider_info_version)
    .load(provider_info_path)
)

readmission_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", readmission_version)
    .load(readmission_path)
)

# Stores the path, captured vrsion, and affected-key columns for each table
silver_tables = {
    "staffing": {
        "path": staffing_path,
        "current_version": staffing_version,
        "impact_columns": ["CCN"]
    },
    "provider_info": {
        "path": provider_info_path,
        "current_version": provider_info_version,
        "impact_columns": ["CCN"]
    },
    "readmission_performance": {
        "path": readmission_path,
        "current_version": readmission_version,
        "impact_columns": ["CCN"]
    }
}

# Read the Gold processing checkpoints
gold_checkpoint_path = f"s3://{bucket_name}/control/gold_checkpoints/"

checkpoint_df = (
    spark.read
    .format("delta")
    .load(gold_checkpoint_path)
)

# --------------------------------------------------
# 4. # Define how to retrieve unprocessed Silver changes using Change Data Feed; identifies affected facilities per table
# --------------------------------------------------

def read_silver_changes(
    silver_table_name,
    table_config,
    checkpoint_df
):

    # 1. Retrieve the last processed version from the Gold checkpoint
    checkpoint_row = (
        checkpoint_df
        .filter(col("silver_table") == silver_table_name)
        .select("last_processed_version")
        .first()
    )

    if checkpoint_row is None:
        raise ValueError(
            f"No Gold checkpoint found for {silver_table_name}"
        )

    last_processed_version = checkpoint_row["last_processed_version"]

    # 2. Retrieve the captured current Silver version
    current_version = table_config["current_version"]

    print(
        f"{silver_table_name}: "
        f"Last processed version = {last_processed_version} | "
        f"Current version = {current_version}"
    )

    # 3. Check whether there are unprocessed Silver changes
    if current_version > last_processed_version:

        # Read only the unprocessed version range
        changes_df = (
            spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", last_processed_version + 1)
            .option("endingVersion", current_version)
            .load(table_config["path"])
        )

        # 4. Identify the affected facilities
        affected_keys_df = (
            changes_df
            .select(*table_config["impact_columns"])
            .distinct()
        )

        print(
            f"{silver_table_name}: "
            f"Unprocessed changes found."
        )

    else:
        changes_df = None
        affected_keys_df = None

        print(
            f"{silver_table_name}: "
            f"No unprocessed changes found."
        )
        

    # 5. Return the changes and version information
    return {
        "silver_table": silver_table_name,
        "last_processed_version": last_processed_version,
        "current_version": current_version,
        "changes_df": changes_df,
        "affected_keys_df": affected_keys_df
    }
    
# --------------------------------------------------
# 5. Combines the affected facilities per table so that Section 6 can use one common list
# --------------------------------------------------

silver_changes = {}

for silver_table_name, table_config in silver_tables.items():

    result = read_silver_changes(
        silver_table_name,
        table_config,
        checkpoint_df
    )

    silver_changes[silver_table_name] = result
    
# Start with no combined DataFrame.
affected_ccns_df = None

for table_changes in silver_changes.values():

    # Retrieve each table's affected_keys_df
    table_affected_ccns_df = table_changes["affected_keys_df"]

    # Add it to the combined DataFrame
    if table_affected_ccns_df is not None:

        if affected_ccns_df is None:
            affected_ccns_df = table_affected_ccns_df
        else:
            affected_ccns_df = affected_ccns_df.unionByName(
                table_affected_ccns_df
            )
# Remove duplicates.
if affected_ccns_df is not None:
    affected_ccns_df = affected_ccns_df.distinct()

# --------------------------------------------------
# 6. Build Table 1: facility_analytical
# Filter and Aggregate staffing data by facility
# --------------------------------------------------

if affected_ccns_df is not None:

    # Filter staffing records to Q2 2024
    staffing_q2_df = staffing_df.filter(
        (col("work_date") >= lit("2024-04-01").cast("date")) &
        (col("work_date") <= lit("2024-06-30").cast("date"))
    )

    # Keep only staffing records for affected facilities
    staffing_q2_affected_df = staffing_q2_df.join(
        affected_ccns_df,
        on="CCN",
        how="left_semi"
    )

    # Aggregate daily staffing records by facility
    staffing_facility_df = (
        staffing_q2_affected_df
        .groupBy("CCN", "state")
        .agg(
            count("work_date").alias("reported_days"),

            spark_sum("mds_census").alias("total_resident_days"),

            spark_sum("hrs_rn").alias("total_rn_hours"),

            spark_sum(
                when(col("hrs_lpn_anomaly") == False, col("hrs_lpn"))
            ).alias("total_lpn_hours"),

            spark_sum("hrs_cna").alias("total_cna_hours"),

            spark_sum("hrs_na_trn").alias("total_na_trn_hours"),

            spark_sum("hrs_med_aide").alias("total_med_aide_hours")
        )
    )

else:
    staffing_facility_df = None

# --------------------------------------------------
# 7. Build Table 1: facility_analytical
# Calculate facility-level staffing metrics
# --------------------------------------------------

if staffing_facility_df is not None:

    # Calculate average daily census and staffing hours per resident-day
    staffing_facility_df = (
        staffing_facility_df

        .withColumn(
            "average_daily_census",
            col("total_resident_days") / 91
        )

        .withColumn(
            "rn_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_rn_hours") / col("total_resident_days")
            )
        )

        .withColumn(
            "lpn_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_lpn_hours") / col("total_resident_days")
            )
        )

        .withColumn(
            "cna_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_cna_hours") / col("total_resident_days")
            )
        )

        .withColumn(
            "na_trn_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_na_trn_hours") / col("total_resident_days")
            )
        )

        .withColumn(
            "med_aide_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_med_aide_hours") / col("total_resident_days")
            )
        )
    )

    # --------------------------------------------------
    # 8. Build Table 1: facility_analytical
    # Join Provider Info and readmission data
    # --------------------------------------------------

    facility_gold_df = (
        staffing_facility_df
        .join(
            provider_info_df.drop("state"),
            on="CCN",
            how="left"
        )
        .join(
            readmission_df.select(
                "CCN",
                "fy_2022_risk_standardized_readmission_rate"
            ),
            on="CCN",
            how="left"
        )
        .withColumn("reporting_period", lit("2024Q2"))
    )

else:
    facility_gold_df = None
    
# --------------------------------------------------
# Identify affected facilities to delete from facility_analytical, Identify affected CCNs that no longer have a qualifying Q2 staffing record.
# --------------------------------------------------

if affected_ccns_df is not None and facility_gold_df is not None:

    facility_ccns_to_delete_df = (
        affected_ccns_df
        .join(
            facility_gold_df.select("CCN").distinct(),
            on="CCN",
            how="left_anti"
        )
    )

else:
    facility_ccns_to_delete_df = None 
# --------------------------------------------------
# 9. Build Table 2: state_staffing_summary  
# --------------------------------------------------
has_staffing_changes = (
    silver_changes["staffing"]["changes_df"] is not None
)

has_provider_info_changes = (
    silver_changes["provider_info"]["changes_df"] is not None
)

if has_staffing_changes or has_provider_info_changes:
    # Aggregate staffing by state
    state_staffing_df = (
        staffing_q2_df
        .groupBy("state")
        .agg(
            count("CCN").alias("reported_facility_days"),
            spark_sum("mds_census").alias("total_resident_days"),
            spark_sum("hrs_rn").alias("total_rn_hours"),
            spark_sum(
                when(col("hrs_lpn_anomaly") == False, col("hrs_lpn"))
            ).alias("total_lpn_hours"),
            spark_sum("hrs_cna").alias("total_cna_hours"),
            spark_sum("hrs_na_trn").alias("total_na_trn_hours"),
            spark_sum("hrs_med_aide").alias("total_med_aide_hours")
        )
    )
    
    #Calculate state staffing rates 
    state_staffing_df = (
        state_staffing_df
        .withColumn(
            "rn_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_rn_hours") / col("total_resident_days")
            )
        )
        .withColumn(
            "lpn_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_lpn_hours") / col("total_resident_days")
            )
        )
        .withColumn(
            "cna_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_cna_hours") / col("total_resident_days")
            )
        )
        .withColumn(
            "na_trn_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_na_trn_hours") / col("total_resident_days")
            )
        )
        .withColumn(
            "med_aide_hours_per_resident_day",
            when(
                col("total_resident_days") > 0,
                col("total_med_aide_hours") / col("total_resident_days")
            )
        )
    )
    
    # Aggregate bed utilization by state
    provider_eligible_df = provider_info_df.filter(
        (col("residents_per_day_missing") == False) &
        col("average_residents_per_day").isNotNull() &
        (col("certified_beds") > 0)
    )
    
    state_bed_utilization_df = (
        provider_eligible_df
        .groupBy("state")
        .agg(
            count("CCN").alias("bed_utilization_facility_count"),
            spark_sum("average_residents_per_day").alias(
                "total_average_daily_residents"
            ),
            spark_sum("certified_beds").alias("total_certified_beds")
        )
    )
    
    state_bed_utilization_df = (
        state_bed_utilization_df
        .withColumn(
            "bed_utilization_percent",
            when(
                col("total_certified_beds") > 0,
                (
                    col("total_average_daily_residents") /
                    col("total_certified_beds")
                ) * 100
            )
        )
    )
    
    # Combine state-level datasets
    state_summary_df = (
        state_staffing_df
        .join(
            state_bed_utilization_df,
            on="state",
            how="full_outer"
        )
        .withColumn("reporting_period", lit("2024Q2"))
    )
else:
    state_summary_df = None

# --------------------------------------------------
# 10. Build Table 3: staffing_readmission_correlation
# --------------------------------------------------
has_staffing_changes = (
    silver_changes["staffing"]["changes_df"] is not None
)

has_readmission_changes = (
    silver_changes["readmission_performance"]["changes_df"] is not None
)

if has_staffing_changes or has_readmission_changes:
    staffing_readmission_df = staffing_facility_df.join(
        readmission_df.select(
            "CCN",
            "provider_name",
            "fy_2022_risk_standardized_readmission_rate"
        ),
        on="CCN",
        how="inner"
    )
    
    staffing_readmission_gold_df = (
        staffing_readmission_df
        .withColumn("reporting_period", lit("2024Q2"))
        .select(
            "CCN",
            "provider_name",
            "state",
            "reporting_period",
            "rn_hours_per_resident_day",
            "lpn_hours_per_resident_day",
            "cna_hours_per_resident_day",
            "fy_2022_risk_standardized_readmission_rate"
        )
    )
else:
    staffing_readmission_gold_df = None

# --------------------------------------------------
# Identify affected facilities to delete from correlation
# --------------------------------------------------

if affected_ccns_df is not None and staffing_readmission_gold_df is not None:

    correlation_ccns_to_delete_df = (
        affected_ccns_df
        .join(
            staffing_readmission_gold_df.select("CCN").distinct(),
            on="CCN",
            how="left_anti"
        )
    )

else:
    correlation_ccns_to_delete_df = None
    
# --------------------------------------------------
# 11. Write Gold tables and validate persistence
# --------------------------------------------------
# Gold output paths
facility_gold_path = f"s3://{bucket_name}/gold/facility_analytical/"
state_summary_path = f"s3://{bucket_name}/gold/state_summary/"
correlation_gold_path = (
    f"s3://{bucket_name}/gold/staffing_readmission_correlation/"
)

# Write a Gold table and verify the persisted row count
def write_and_validate_gold(df, output_path, table_name):

    # Count the records intended for the Gold table
    expected_count = df.count()

    # Write the DataFrame to Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)

    # Read the persisted Delta table and count its records
    persisted_df = spark.read \
        .format("delta") \
        .load(output_path)

    actual_count = persisted_df.count()

    # Confirm that the persisted record count matches expectations
    if actual_count != expected_count:
        raise RuntimeError(
            f"FAIL: {table_name} - "
            f"Expected {expected_count} rows, "
            f"but found {actual_count} rows."
        )

    print(
        f"PASS: {table_name} - "
        f"Write succeeded. "
        f"Expected rows: {expected_count}; "
        f"Persisted rows: {actual_count}."
    )

# --------------------------------------------------
# WRITE AND VALIDATE ALL THREE GOLD TABLES 
# --------------------------------------------------

# --------------------------------------------------
# Write Table 1: facility_analytical
# Upsert affected facility records
# --------------------------------------------------

if facility_gold_df is not None:

    expected_facility_count = facility_gold_df.count()

    if expected_facility_count > 0:

        target = DeltaTable.forPath(
            spark,
            facility_gold_path
        )

        # 1. Build the matching condition.
        merge_condition = "target.`CCN` = source.`CCN`"

        # 2. Identify all non-key columns to compare.
        comparison_columns = [
            column
            for column in facility_gold_df.columns
            if column != "CCN"
        ]

        # 3. Build the null-safe conditional update.
        change_condition = "NOT (" + " AND ".join(
            [
                f"target.`{column}` <=> source.`{column}`"
                for column in comparison_columns
            ]
        ) + ")"

        # 4. Match incoming records against Gold.
        merge = target.alias("target").merge(
            facility_gold_df.alias("source"),
            merge_condition
        )

        # 5. Update existing records only when values changed.
        merge = merge.whenMatchedUpdateAll(
            condition=change_condition
        )

        # 6. Insert new records.
        merge = merge.whenNotMatchedInsertAll()

        # 7. Execute the MERGE.
        merge.execute()

        print(
            f"Facility Analytical MERGE completed. "
            f"Records submitted: {expected_facility_count}."
        )

    else:
        print("No facility records to insert or update.")

else:
    print("No Silver changes affecting Facility Analytical.")

# --------------------------------------------------
# Delete affected facilities no longer in facility_analytical
# --------------------------------------------------

if facility_ccns_to_delete_df is not None:

    target = DeltaTable.forPath(
        spark,
        facility_gold_path
    )

    delete_source_df = (
        facility_ccns_to_delete_df
        .select("CCN")
        .distinct()
    )

    merge = target.alias("target").merge(
        delete_source_df.alias("source"),
        "target.CCN = source.CCN"
    )

    merge = merge.whenMatchedDelete()

    merge.execute()

    print("Facility Analytical targeted deletion completed.")

else:
    print("No facilities identified for deletion.")

# --------------------------------------------------
# Validate persisted Facility Analytical records
# --------------------------------------------------

if affected_ccns_df is not None:

    persisted_facility_df = (
        spark.read
        .format("delta")
        .load(facility_gold_path)
    )

    # Count affected records remaining in Gold.
    actual_affected_count = (
        persisted_facility_df
        .join(
            affected_ccns_df,
            on="CCN",
            how="left_semi"
        )
        .count()
    )

    # Count affected records expected to remain in Gold.
    expected_affected_count = (
        facility_gold_df.count()
        if facility_gold_df is not None
        else 0
    )

    if actual_affected_count != expected_affected_count:
        raise RuntimeError(
            "FAIL: Facility Analytical validation - "
            f"Expected {expected_affected_count} affected records, "
            f"but found {actual_affected_count}."
        )

    print(
        "PASS: Facility Analytical validation - "
        f"Expected affected records: {expected_affected_count}; "
        f"Persisted affected records: {actual_affected_count}."
    )

else:
    print("No Facility Analytical changes to validate.")
    
# --------------------------------------------------
# Write Table 2: state_summary
# Rebuild the complete state-level summary
# --------------------------------------------------

if state_summary_df is not None:

    write_and_validate_gold(
        state_summary_df,
        state_summary_path,
        "State Summary"
    )

else:
    print(
        "No staffing or Provider Info changes. "
        "State Summary does not need to be rebuilt."
    )

# --------------------------------------------------
# Write Table 3: staffing_readmission_correlation
# Upsert affected facility records
# --------------------------------------------------

if staffing_readmission_gold_df is not None:

    expected_correlation_count = (
        staffing_readmission_gold_df.count()
    )

    if expected_correlation_count > 0:

        target = DeltaTable.forPath(
            spark,
            correlation_gold_path
        )

        # 1. Build the matching condition.
        merge_condition = "target.`CCN` = source.`CCN`"

        # 2. Identify all non-key columns to compare.
        comparison_columns = [
            column
            for column in staffing_readmission_gold_df.columns
            if column != "CCN"
        ]

        # 3. Build the null-safe conditional update.
        change_condition = "NOT (" + " AND ".join(
            [
                f"target.`{column}` <=> source.`{column}`"
                for column in comparison_columns
            ]
        ) + ")"

        # 4. Match incoming records against Gold.
        merge = target.alias("target").merge(
            staffing_readmission_gold_df.alias("source"),
            merge_condition
        )

        # 5. Update existing records only when values changed.
        merge = merge.whenMatchedUpdateAll(
            condition=change_condition
        )

        # 6. Insert new records.
        merge = merge.whenNotMatchedInsertAll()

        # 7. Execute the MERGE.
        merge.execute()

        print(
            "Staffing/Readmission Correlation MERGE completed. "
            f"Records submitted: {expected_correlation_count}."
        )

    else:
        print(
            "No correlation records to insert or update."
        )

else:
    print(
        "No staffing or readmission changes affecting correlation."
    )

# --------------------------------------------------
# Delete affected facilities no longer in correlation
# --------------------------------------------------

if correlation_ccns_to_delete_df is not None:

    target = DeltaTable.forPath(
        spark,
        correlation_gold_path
    )

    delete_source_df = (
        correlation_ccns_to_delete_df
        .select("CCN")
        .distinct()
    )

    merge = target.alias("target").merge(
        delete_source_df.alias("source"),
        "target.CCN = source.CCN"
    )

    merge = merge.whenMatchedDelete()

    merge.execute()

    print(
        "Staffing/Readmission Correlation "
        "targeted deletion completed."
    )

else:
    print(
        "No correlation facilities identified for deletion."
    )

# --------------------------------------------------
# Validate persisted Staffing/Readmission Correlation
# --------------------------------------------------

if staffing_readmission_gold_df is not None:

    persisted_correlation_df = (
        spark.read
        .format("delta")
        .load(correlation_gold_path)
    )

    # Count affected records remaining in Gold.
    actual_correlation_count = (
        persisted_correlation_df
        .join(
            affected_ccns_df,
            on="CCN",
            how="left_semi"
        )
        .count()
    )

    # Count affected records expected to remain in Gold.
    expected_correlation_count = (
        staffing_readmission_gold_df.count()
    )

    if actual_correlation_count != expected_correlation_count:
        raise RuntimeError(
            "FAIL: Staffing/Readmission Correlation validation - "
            f"Expected {expected_correlation_count} affected records, "
            f"but found {actual_correlation_count}."
        )

    print(
        "PASS: Staffing/Readmission Correlation validation - "
        f"Expected affected records: {expected_correlation_count}; "
        f"Persisted affected records: {actual_correlation_count}."
    )

else:
    print(
        "No correlation changes to validate."
    )

# --------------------------------------------------
# Update Gold checkpoints after all writes succeed
# --------------------------------------------------

# 1. Identify Silver tables with unprocessed versions.
checkpoint_updates = [
    (
        table_name,
        table_changes["current_version"]
    )
    for table_name, table_changes in silver_changes.items()
    if (
        table_changes["current_version"]
        > table_changes["last_processed_version"]
    )
]

if checkpoint_updates:

    # 2. Create the checkpoint updates DataFrame.
    checkpoint_updates_df = spark.createDataFrame(
        checkpoint_updates,
        schema="""
            silver_table STRING,
            last_processed_version LONG
        """
    )

    # 3. Access the existing Gold checkpoint table.
    checkpoint_delta_table = DeltaTable.forPath(
        spark,
        gold_checkpoint_path
    )

    # 4. Match checkpoint records by Silver table name.
    merge = checkpoint_delta_table.alias("target").merge(
        checkpoint_updates_df.alias("source"),
        "target.silver_table = source.silver_table"
    )

    # 5. Advance checkpoints only to newer processed versions.
    merge = merge.whenMatchedUpdate(
        condition=(
            "source.last_processed_version "
            "> target.last_processed_version"
        ),
        set={
            "last_processed_version":
                "source.last_processed_version"
        }
    )

    # 6. Execute the checkpoint update.
    merge.execute()

    # 7. Read the persisted checkpoint table.
    persisted_checkpoint_df = (
        spark.read
        .format("delta")
        .load(gold_checkpoint_path)
    )

    # 8. Compare persisted values with expected updates.
    checkpoint_comparison_df = (
        persisted_checkpoint_df.alias("target")
        .join(
            checkpoint_updates_df.alias("source"),
            on="silver_table",
            how="inner"
        )
    )

    matched_checkpoint_count = checkpoint_comparison_df.count()

    mismatched_checkpoint_count = (
        checkpoint_comparison_df
        .filter(
            col("target.last_processed_version")
            != col("source.last_processed_version")
        )
        .count()
    )

    if (
        matched_checkpoint_count != len(checkpoint_updates)
        or mismatched_checkpoint_count != 0
    ):
        raise RuntimeError(
            "FAIL: Gold checkpoint validation. "
            "Persisted checkpoint values do not match "
            "the expected processed Silver versions."
        )

    print(
        "PASS: Gold checkpoint validation. "
        f"Updated and verified {len(checkpoint_updates)} Silver checkpoints."
    )

else:
    print(
        "No unprocessed Silver versions. "
        "Gold checkpoints remain unchanged."
    )

print("INCREMENTAL GOLD JOB COMPLETED SUCCESSFULLY.")



