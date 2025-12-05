from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# --- CONFIGURATION ---
# Auto-detect the folder where this DAG file is located
# This ensures it works on Windows 'C:\Users\...' without manual editing
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_FOLDER = os.path.join(CURRENT_DIR, "scripts")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'covid_19_existing_scripts_pipeline',
    default_args=default_args,
    description='Runs existing ETL scripts (global5050, dies, vac) without modification',
    schedule_interval='@once',  # Run exactly once
    start_date=datetime(2024, 1, 1), # Use a static past date instead of
    catchup=False,
    tags=['covid', 'legacy', 'bash'],
) as dag:

    # Task 1: Run global5050.py
    # This runs: python C:/Users/.../scripts/global5050.py
    t_global5050 = BashOperator(
        task_id='run_global5050_script',
        bash_command=f'python "{os.path.join(SCRIPTS_FOLDER, "global5050.py")}"'
    )

    # Task 2: Run dies.py
    # This runs: python C:/Users/.../scripts/dies.py
    t_disease_sh = BashOperator(
        task_id='run_disease_sh_script',
        bash_command=f'python "{os.path.join(SCRIPTS_FOLDER, "dies.py")}"'
    )

    # Task 3: Run vac.py
    t_vaccinations = BashOperator(
        task_id='run_vaccinations_script',
        bash_command=f'cd "{SCRIPTS_FOLDER}" && python vac.py'
    )

    # Run them in parallel (reducing run time)
    [t_global5050, t_disease_sh, t_vaccinations]


with DAG(
    'covid_19_incremental_load_scripts',
) as il_dag:

    il_global5050 = BashOperator()