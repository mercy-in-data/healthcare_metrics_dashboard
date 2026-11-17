# Healthcare Metrics Dashboard

An end-to-end data engineering project using CMS nursing home data. The project takes raw healthcare data, processes it through an AWS pipeline, and makes the results easier to explore through a Streamlit dashboard.

## About the Project

The source data comes from CMS nursing home datasets stored in Google Drive. AWS Glue processes the files and organizes the data into Bronze, Silver, and Gold layers in Amazon S3.

The dashboard uses the Gold tables to display four metrics:

- **Bed Utilization by State** – How much of the available nursing home bed capacity is being used in each state.
- **Facility Staffing Distribution** – How staffing levels vary across nursing homes by staff type.
- **Staffing and Readmissions** – Whether there is an association between staffing levels and readmission rates.
- **Staffing Intensity by State** – How staffing hours per resident day compare across states.

## The Pipeline

Google Drive → AWS Glue → Amazon S3 (Bronze → Silver → Gold) → Streamlit

The pipeline uses full snapshots of the source files. Delta MERGE operations update the Silver tables, while Change Data Feed (CDF) tracks changes that need to be carried into Gold. 

## Tools and Technologies

- AWS S3, Glue, and Secrets Manager
- Python and PySpark
- Delta Lake
- Streamlit, Pandas, and Matplotlib

## Repository Contents

- `diagrams/` – Architecture and workflow diagrams
- `documentation/` – Project documentation and metric details
- `glue_jobs/` – Python scripts used by AWS Glue
- `app.py` – Streamlit dashboard
- `data_loader.py` – Loads the Gold data from S3
- `requirements.txt` – Python packages needed to run the app

The dashboard is deployed through Streamlit Community Cloud. 
