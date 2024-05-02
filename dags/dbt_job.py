from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtDocsGenerateOperator
)
from airflow.utils.dates import days_ago

default_args = {
  'dir': '/tmp/sample_sales',
  'owner': 'airflow',
  'start_date': days_ago(0)
}

with DAG(dag_id='dbt', default_args=default_args, schedule_interval='@daily') as dag:

  cli_command = BashOperator(
        task_id="Copy_From_S3",
        bash_command="cp -R /usr/local/airflow/dags/sample_sales /tmp;"
  )

  
  dbt_docs = DbtDocsGenerateOperator(
    task_id='dbt_docs',
  )

  dbt_run = DbtRunOperator(
    task_id='dbt_run',
  )

  dbt_test = DbtTestOperator(
    task_id='dbt_test',
    retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
  )

  cli1_command = BashOperator(
        task_id="Pust_to_S3",
        bash_command="ls -ltrR /tmp/sample_sales;\
        aws s3 cp /tmp/sample_sales/target/ s3://mwaa-160071257600-20240429072058028500000003/dags/sample_sales/target/ --recursive --exclude '*' --include '*.json' --debug;"
  )

  cli_command >> dbt_docs >> dbt_run >> dbt_test >> cli1_command
