import sys
import json
import boto3
from botocore.exceptions import ClientError
import uuid

from awsglue.utils import getResolvedOptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import io


# --------------------------------------------------
# 1. Read Glue job parameters
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["secret_name", "drive_folder_id"]
)

secret_name = args["secret_name"]
folder_id = args["drive_folder_id"]


# --------------------------------------------------
# 2. Retrieve Google service-account credentials
# --------------------------------------------------

secrets_client = boto3.client(
    "secretsmanager",
    region_name="us-east-1"
)

secret_response = secrets_client.get_secret_value(
    SecretId=secret_name
)

secret_data = json.loads(
    secret_response["SecretString"]
)

service_account_info = json.loads(
    secret_data["glue_drive_ingestion"]
)


# --------------------------------------------------
# 3. Authenticate to Google Drive
# --------------------------------------------------

scopes = [
    "https://www.googleapis.com/auth/drive.readonly"
]

credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=scopes
)

drive = build(
    "drive",
    "v3",
    credentials=credentials,
    cache_discovery=False
)


# --------------------------------------------------
# 4. Configure Amazon S3
# --------------------------------------------------

s3 = boto3.client("s3")

bucket_name = "mb-healthcare-metrics"

bronze_prefix = "bronze/"


# --------------------------------------------------
# 5. List CSV files in the Google Drive folder
# --------------------------------------------------

query = (
    f"'{folder_id}' in parents "
    "and trashed = false"
)

files = []
page_token = None

while True:

    response = drive.files().list(
        q=query,
        pageSize=100,
        pageToken=page_token,
        fields="nextPageToken,files(id,name,mimeType,modifiedTime)"
    ).execute()

    files.extend(response.get("files", []))

    page_token = response.get("nextPageToken")

    if not page_token:
        break

files = [
    file for file in files
    if file["name"].lower().endswith(".csv")
]

print(f"Total CSV files found: {len(files)}")

# --------------------------------------------------
# 6. Read the registry and create a set of processed IDs
# --------------------------------------------------

response = s3.get_object(
    Bucket=bucket_name,
    Key="state/processed_file_registry.json"
)

registry_content = response["Body"].read().decode("utf-8")
registry = json.loads(registry_content)

processed_file_ids = {
    entry
    for entry in registry["processed_files"]
}
# --------------------------------------------------
# 7. Download and upload each CSV file
# --------------------------------------------------

successful_uploads = 0
failed_uploads = 0
files_for_manifest = []

# Identify the destination based on the dataset filename pattern
def identify_dataset(file_name):
    if file_name.startswith("PBJ_Daily_Nurse_Staffing_"):
        return "staffing"

    elif file_name.startswith("NH_ProviderInfo_"):
        return "provider_info"

    elif (
        file_name.startswith("FY_")
        and "_SNF_VBP_Facility_Performance" in file_name
    ):
        return "readmission_performance"

    return None

for file in files:

    file_id = file["id"]
    file_name = file["name"]
    dataset_type = identify_dataset(file_name)

    if dataset_type is None:
        print(f"Skipping unrelated file: {file_name}")
        continue

    target_silver_table = dataset_type

    # Registry Check --> Skip files already processed into Silver
    if file_id in processed_file_ids:
        print(f"Already processed into Silver: {file_name}")
        continue

    # Bronze Check --> Check whether the Bronze object already exists because it failed Silver processing
    s3_key = f"{bronze_prefix}{file_id}_{file_name}"

    try:
        s3.head_object(
            Bucket=bucket_name,
            Key=s3_key
        )

        print(f"Bronze object already exists: {file_name}")

        bronze_ready = True

    except ClientError as error:

        if error.response["Error"]["Code"] == "404":

            print(f"Downloading new file: {file_name}")

            try:
                # Download original file contents from Google Drive
                request = drive.files().get_media(
                    fileId=file_id
                )

                file_buffer = io.BytesIO()

                downloader = MediaIoBaseDownload(
                    file_buffer,
                    request
                )

                done = False

                while not done:
                    status, done = downloader.next_chunk()

                file_buffer.seek(0)

                # Upload original contents to S3
                s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=file_buffer.getvalue()
                )

                successful_uploads += 1
                bronze_ready = True

                print(
                    f"SUCCESS: {file_name} uploaded to "
                    f"s3://{bucket_name}/{s3_key}"
                )

            except Exception as error:

                failed_uploads += 1
                bronze_ready = False

                print(
                    f"FAILED: {file_name} | "
                    f"Error: {str(error)}"
                )

        else:
            raise

    if bronze_ready:
        files_for_manifest.append({
            "drive_file_id": file_id,
            "file_name": file_name,
            "bronze_s3_key": s3_key,
            "dataset_type": dataset_type,
            "target_silver_table": target_silver_table
        })

# --------------------------------------------------
# 8. Failure Check: Stop with a job failure if any relevant download + upload fails.
# --------------------------------------------------

if failed_uploads > 0:
    raise RuntimeError(
        f"{failed_uploads} file(s) failed to download. "
        "Stopping the job before creating the manifest."
    )
        
# --------------------------------------------------
# 9. Create or update the pending batch manifest
# --------------------------------------------------

manifest = {"files": files_for_manifest}

manifest_json = json.dumps(manifest, indent=2)

s3.put_object(
    Bucket=bucket_name,
    Key="manifests/current_batch.json",
    Body=manifest_json,
    ContentType="application/json"
)

if files_for_manifest:
    print(f"Manifest created with {len(files_for_manifest)} files.")
else:
    print("No new files available for Silver processing.")

# --------------------------------------------------
# 10. Print ingestion summary
# --------------------------------------------------

print("--------------------------------")
print("INGESTION SUMMARY")
print("--------------------------------")
print(f"Files discovered: {len(files)}")
print(f"Successful uploads: {successful_uploads}")
print(f"Failed uploads: {failed_uploads}")
print("Ingestion process completed.")

