import os
from contextlib import closing
from datetime import timedelta

import pendulum
import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from psycopg2 import sql


DAG_ID = "gold_realestate_aggregate"
GOLD_TABLES = (
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
)
start_month = os.getenv("REAL_ESTATE_START_MONTH", "2026-09")
has_credentials = all(
    os.getenv(name)
    for name in ("DATA_GO_KR_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
)


def verify_gold(data_interval_start):
    month = int(data_interval_start.in_timezone("Asia/Seoul").format("YYYYMM"))
    dsn = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cursor:
        for table in GOLD_TABLES:
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE yyyymm = %s").format(
                    sql.Identifier("public", table)
                ),
                (month,),
            )
            count = cursor.fetchone()[0]
            print(f"{table} | yyyymm={month} | rows={count}")
            if count <= 0:
                raise AirflowException(f"{table}: no rows for {month}")


with DAG(
    dag_id=DAG_ID,
    schedule="@monthly",
    catchup=True,
    start_date=pendulum.from_format(f"{start_month}-01", "YYYY-MM-DD", tz="Asia/Seoul"),
    max_active_runs=1,
    is_paused_upon_creation=not has_credentials,
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        poke_interval=30,
        timeout=7200,
    )

    run_gold_spark = BashOperator(
        task_id="run_gold_spark",
        bash_command=r"""
        set -euo pipefail
        exec "$SPARK_HOME/bin/spark-submit" \
          --master 'local[2]' \
          --name gold_realestate_aggregate \
          --conf spark.ui.enabled=false \
          --conf spark.sql.shuffle.partitions=8 \
          --conf spark.jars.ivy=/tmp/realestate-gold-ivy \
          --repositories https://repo.maven.apache.org/maven2 \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.8 \
          /opt/airflow/project/Q3/scripts/gold_spark_sql.py \
          --month '{{ data_interval_start.in_timezone("Asia/Seoul").format("YYYYMM") }}'
        """,
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=30),
    )

    verify_postgres = PythonOperator(
        task_id="verify_postgres",
        python_callable=verify_gold,
        do_xcom_push=False,
    )

    wait_for_silver >> run_gold_spark >> verify_postgres
