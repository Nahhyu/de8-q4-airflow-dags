import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


APP_NAME = "silver_realestate_transform"
start_month = os.getenv("REAL_ESTATE_START_MONTH", "2026-09")
has_credentials = all(
    os.getenv(name)
    for name in ("DATA_GO_KR_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
)

with DAG(
    dag_id=APP_NAME,
    schedule="@monthly",
    start_date=pendulum.from_format(f"{start_month}-01", "YYYY-MM-DD", tz="Asia/Seoul"),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=not has_credentials,
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    run_silver_spark = BashOperator(
        task_id="run_silver_spark",
        bash_command=r"""
        set -euo pipefail
        exec "$SPARK_HOME/bin/spark-submit" \
          --master 'local[2]' \
          --name silver_realestate_transform \
          --conf spark.ui.port=4040 \
          --conf spark.driver.bindAddress=0.0.0.0 \
          --conf spark.sql.shuffle.partitions=8 \
          --conf spark.jars.ivy=/tmp/realestate-ivy \
          --repositories https://repo.maven.apache.org/maven2 \
          --packages org.apache.hadoop:hadoop-aws:3.3.4 \
          /opt/airflow/project/Q2/scripts/silver_spark.py \
          --month '{{ data_interval_start.in_timezone("Asia/Seoul").format("YYYYMM") }}'
        """,
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=30),
    )

    wait_for_bronze >> run_silver_spark
