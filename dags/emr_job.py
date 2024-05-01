from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator
from datahub_airflow_plugin.entities import Dataset, Urn
import logging

DAG_ID = "emr_pyspark"
BUCKET_NAME_KEY = "BUCKET_NAME"

LOG_BUCKET = "jcp-emr-log-bucket-160071257600"  # Updated log bucket name

JOB_FLOW_OVERRIDES = {
    "Name": "PiCalc",
    "ReleaseLabel": "emr-7.1.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": True,
        "Ec2SubnetId": "subnet-012de56c8e1bb6b47"
    },
    "JobFlowRole": "arn:aws:iam::160071257600:instance-profile/EMRRoleForInstance",
    "ServiceRole": "arn:aws:iam::160071257600:role/JCP-EMR-ClusterRole",
    "LogUri": f"s3://{LOG_BUCKET}/default_job_flow_location",  # Updated log URI
    "BootstrapActions": [
        {
            "Name": "Install Boto3 and Bootstrap",
            "ScriptBootstrapAction": {
                "Path": "s3://jcp-emr-log-bucket-160071257600/script/bootstrap.sh"
            }
        }
    ]
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=["emr"],
) as dag:

    def on_success(context):
        logging.info("EMR cluster creation successful")

    def on_failure(context):
        logging.error("EMR cluster creation failed with exception: %s", context['exception'])

    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        on_success_callback=on_success,
        on_failure_callback=on_failure,
    )

    spark_step = {
        'Name': 'Run Spark',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--packages', 'net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,io.acryl:acryl-spark-lineage:0.1.0', '--py-files', 's3://pyspark-160071257600/spark_app.zip','s3://pyspark-160071257600/spark_main.py'],
        },
    }

    add_step = EmrAddStepsOperator(
        task_id='add_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        steps=[spark_step],
        aws_conn_id='aws_default',
        dag=dag,
    )

    create_job_flow >> add_step
