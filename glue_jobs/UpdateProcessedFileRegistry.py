import sys
import json
import boto3

from awsglue.utils import getResolvedOptions

# Retrieve job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "bucket_name",
        "manifest_key",
        "registry_key"
    ]
)

bucket_name = args["bucket_name"]
manifest_key = args["manifest_key"]
registry_key = args["registry_key"]

# Initialize S3 client
s3 = boto3.client("s3")

# --------------------------------------------------
# 1. Read the current batch manifest
# --------------------------------------------------

manifest_response = s3.get_object(
    Bucket=bucket_name,
    Key=manifest_key
)

manifest = json.loads(
    manifest_response["Body"].read().decode("utf-8")
)

# Extract the New Google Drive file IDs from the manifest
new_file_ids = [
    file["drive_file_id"]
    for file in manifest["files"]
]

print(f"Found {len(new_file_ids)} files in the current manifest.")


# --------------------------------------------------
# 2. Read the existing processed-file registry
# --------------------------------------------------

registry_response = s3.get_object(
    Bucket=bucket_name,
    Key=registry_key
)

registry = json.loads(
    registry_response["Body"].read().decode("utf-8")
)

processed_file_ids = registry["processed_files"]

print(f"Found {len(processed_file_ids)} previously processed files.")

# --------------------------------------------------
# 3. Combine both lists without duplicates
# --------------------------------------------------

updated_file_ids = list(
    dict.fromkeys(processed_file_ids + new_file_ids)
)

print(f"Registry now contains {len(updated_file_ids)} unique file IDs.")

# --------------------------------------------------
# 4. Write theupdated processed-file registry to S3
# --------------------------------------------------

updated_registry = {
    "processed_files": updated_file_ids
}

registry_json = json.dumps(updated_registry, indent=2)

s3.put_object(
    Bucket=bucket_name,
    Key=registry_key,
    Body=registry_json,
    ContentType="application/json"
)

print("Processed-file registry successfully updated.")

