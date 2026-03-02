# COVID-19 Vaccine Analysis
This project implements a comprehensive data engineering pipeline to collect, process, and store multi-source COVID-19 data for long-term vaccine effectiveness evaluation.

# 1. System Architecture
The pipeline is designed with a focus on **automation, scalability, and reliability**, ultilizing a modern data stack:
  - **Environment:** Orchestrated using **Docker** to ensure consistency across development and production.
  - **Orchestration:** Managed by **Apache Airflow**, featuring Directed Acyclic Graphs (DAGs) for both full and incremental data loading.
  - **Storage:** A **MongoDB** (NoSQL) database servers as the primary data store, chosen for its native support for nested JSON structures from APIs.

# 2. Data Sources & Extraction
The system integrates data from three primary high-reputation open sources:
  - **Our World in Data (CSV):** Historical vaccination records for 235 countries.
  - **diseash.sh (API):** Real-time global time-series data for cases, deaths and recovery.
  - **api.global5050.org (API):** Specialized demographic data focusing on age and sex distribution.

# 3. Data Processing & Transformation
The pipeline ultilizes **Python** and **Pandas** for rigorous data cleaning and normalization:

**Handle Missing Value**
  - **Forwarding-Fill (ffill):** Applied to time-series vaccination data. This is assumes that if a report is missing for a day, the status remains unchanged from the last valid record.
  - **Zero-Filling:** Remaining gaps are filled with `0` to maintain dataset integrity without introducing statistical noise.

**Normaliztion & Validation**
  - **Date Standardization:** Converging various date formats from different APIs into a unified `datetime` format.
  - **Duplicate Removal:** Logic-based deduplication to ensure record uniqueness.
  - **Outlier Detection:** Ultizing **Z-Score** statistics to identify and handle anomalies like negative vaccination counts.

# 4. Load Strategy: Incremental vs. Full Load
To optimize performance and reduce network overhead, the project implements two loading scenarios:
  - **Full Load:** Executed once to initialize the historical database.
  - **Incremental Load:** Scheduled every 30 days. It use an **Upsert** (Update/Insert) mechanism via MongoDB's `bulk_write` to update existing records or add new ones without regenerating the entire dataset.
  - **Idempotency:** The pipeline is designed to be self-healing; it can be re-run multiple times without creating duplicate "junk" data.

# 5. Technology Selection Justification
| Criteria | Selection | Reason |
|:---|:---|:---|
| **Database** | MongoDB | Superior handling of nested JSON; flexible schema for evolving API data. |
| **Processing** | Pandas | Efficent handling of large CSVs and complex transformation. |
| **Orchestration** | Airflow | Robust scheduling, error handling, and visual DAG monitoring. |

# 6. Setup
## Create Virtual Environment
`python -m venv venv`
`venv\Scripts\activate`
## Install Requirements
`pip install -r requirements.txt`
# 7. How To Run
## Init Airflow
`docker-compose up --build airflow-init`
## Run Containers
`docker-compose up`
