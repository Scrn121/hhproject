from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}


def extract_task():

	import sys
	sys.path.insert(0, '/opt/airflow')

	os.environ['POSTGRES_HOST'] = os.getenv('HH_POSTGRES_HOST', 'localhost')
	os.environ['POSTGRES_PORT'] = os.getenv('HH_POSTGRES_PORT', '5432')
	os.environ['POSTGRES_DB'] = os.getenv('HH_POSTGRES_DB', 'hh_database')
	os.environ['POSTGRES_USER'] = os.getenv('HH_POSTGRES_USER', 'hh_user')
	os.environ['POSTGRES_PASSWORD'] = os.getenv('HH_POSTGRES_PASSWORD', 'hh_password')

	from extractor.hh_extractor import run
	run()


with DAG(
	dag_id='hh_pipeline',
	default_args=default_args,
	description='ETL pipeline: raw -> staging -> marts',
	schedule_interval='@hourly',
	start_date=datetime(2026, 4, 16),
	catchup=False,
	tags=['hh', 'etl'],
) as dag:

	# Task 1 SRC->RAW
	extract_vacancies = PythonOperator(
		task_id='extract_vacancies',
		python_callable=extract_task,
	)

	# Task 2: RAW->STG
	dbt_staging = BashOperator(
		task_id='dbt_run_staging',
		bash_command='cd /opt/airflow/hh_dbt && dbt run --models stg_vacancies --profiles-dir /opt/airflow/hh_dbt/profiles',
	)

	#task 3: STG->Matrs
	dbt_marts = BashOperator(
		task_id='dbt_run_marts',
		bash_command = 'cd /opt/airflow/hh_dbt && dbt run --models marts --profiles-dir /opt/airflow/hh_dbt/profiles'
	)

	extract_vacancies >> dbt_staging >> dbt_marts