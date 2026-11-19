import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_gitsync",
    description="Kubernetes git-sync 배포 확인 (데엔 8기 배나현)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["q4", "gitsync"],
) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command="echo 'hello from git-sync (데엔 8기 배나현)' && date -Iseconds",
    )
