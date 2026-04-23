from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

#sys.path.insert(0, '/opt/Airflow/extractor')

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
	}

def execute_task():
	os.environ['POSTGRES_HOST'] = os.getenv('HH_POSTGRES_HOST', 'localhost')
	os.environ['POSTGRES_PORT'] = os.getenv('HH_POSTGRES_PORT', '5432')
	os.environ['POSTGRES_DB'] = os.getenv('HH_POSTGRES_DB', 'hh_database')
	os.environ['POSTGRES_USER'] = os.getenv('HH_POSTGRES_USER', 'hh_user')
	os.environ['POSTGRES_PASSWORD'] = os.getenv('HH_POSTGRES_PASSWORD', 'hh_password')

	import sys
	sys.path.insert(0, '/opt/airflow/extractor')
	from hh_extractor import run
	run()

with DAG(
	dag_id = 'hh_pipeline',
	default_args = default_args,
	description = 'RAW load',
	schedule_interval = '@hourly',
	start_date = datetime(2026, 4, 16),
	catchup = False,
	tags = ['hh', 'extract'],
) as dag:

	extract_vacancies = PythonOperator(
		task_id = 'extract_vacancies',
		python_callable = execute_task,
	)

	#extract_vacancies