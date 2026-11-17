# Healthcare Metrics Project
This project takes CMS nursing home data, processes it through an AWS data pipeline, and turns it into a Streamlit dashboard with healthcare metrics.

## About the Project

The pipeline pulls CMS datasets from Google Drive, processes them using AWS Glue, and stores the data in Amazon S3 using a Bronze, Silver, and Gold structure.

The dashboard reads from the Gold tables and displays four metrics:

- **Bed Utilization by State:** Compares how much of the available nursing home bed capacity is being used across states.
- **Facility Staffing Distribution:** Shows how staffing levels vary across nursing homes by staff type.
- **Staffing and Readmissions:** Looks at the relationship between nursing home staffing levels and readmission rates.
- **Staffing Intensity by State:** Compares staffing hours per resident day across states.

## How It Works

Google Drive → AWS Glue → Amazon S3 (Bronze → Silver → Gold) → Streamlit Dashboard

The pipeline uses full-snapshot ingestion and Delta MERGE operations to keep the Silver tables updated. Change Data Feed (CDF) tracks changes so the Gold tables can be updated incrementally.

## Tools Used

- **AWS:** S3, Glue, Secrets Manager
- **Data Processing:** Python, PySpark, Delta Lake
- **Dashboard:** Streamlit, Pandas, Matplotlib
- **Source Data:** CMS Nursing Home datasets

## Repository Structure

- `diagrams/` – Pipeline and architecture diagrams
- `documentation/` – Project notes, data profiling, data quality rules, and metric definitions
- `glue_jobs/` – AWS Glue scripts
- `app.py` – Streamlit dashboard
- `data_loader.py` – Loads data from the Gold tables
- `requirements.txt` – Project dependencies

## Deployment

The dashboard is deployed through Streamlit Community Cloud. 
