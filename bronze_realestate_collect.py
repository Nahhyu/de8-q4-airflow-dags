import base64
import os
from datetime import datetime, timedelta
from xml.etree import ElementTree

import boto3
import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from botocore.exceptions import ClientError


API_URL = (
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/"
    "getRTMSDataSvcAptTrade"
)
DISTRICTS = ("11680", "11650", "11710", "11440", "11170", "11200")
INITIAL_PAGE_SIZE = 1000


def request_xml(api_key, lawd_cd, yyyymm, page_size):
    try:
        response = requests.get(
            API_URL,
            params={
                "serviceKey": api_key,
                "LAWD_CD": lawd_cd,
                "DEAL_YMD": yyyymm,
                "pageNo": 1,
                "numOfRows": page_size,
            },
            timeout=(10, 60),
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        # Requests errors can contain the URL and its serviceKey parameter.
        raise AirflowException(f"API 연결 실패 ({type(exc).__name__})") from None
    if response.status_code != 200:
        raise AirflowException(f"API HTTP 상태 {response.status_code}")
    return response.content


def inspect_xml(raw_xml):
    try:
        root = ElementTree.fromstring(raw_xml)
    except ElementTree.ParseError:
        return "invalid_xml", 0, 0

    result_code = (
        root.findtext("./header/resultCode")
        or root.findtext("./cmmMsgHeader/returnReasonCode")
        or ""
    ).strip()
    if result_code in {"03", "3"}:
        return "empty", 0, 0
    if result_code not in {"000", "00", "0"}:
        safe_code = result_code if result_code.isascii() and result_code.isdigit() else "unknown"
        raise AirflowException(f"공공데이터포털 API 응답 오류 (코드 {safe_code})")

    try:
        total = int(root.findtext("./body/totalCount", ""))
    except ValueError:
        return "invalid_xml", 0, 0

    count = len(root.findall("./body/items/item"))
    if total == 0 and count == 0:
        return "empty", 0, 0
    if total <= 0 or count == 0 or count > total:
        return "invalid_xml", 0, 0
    return "ok", total, count


def collect(lawd_cd, **context):
    print(f"collector=배나현, time={datetime.now()}, lawd={lawd_cd}")
    api_key = os.getenv("DATA_GO_KR_API_KEY", "").strip()
    if not api_key:
        raise AirflowException("DATA_GO_KR_API_KEY를 입력하세요.")

    yyyymm = context["data_interval_start"].in_timezone("Asia/Seoul").format("YYYYMM")
    raw_xml = request_xml(api_key, lawd_cd, yyyymm, INITIAL_PAGE_SIZE)
    status, total, count = inspect_xml(raw_xml)
    if status == "ok" and count < total:
        raw_xml = request_xml(api_key, lawd_cd, yyyymm, total)
        status, total, count = inspect_xml(raw_xml)
        if status == "ok" and count != total:
            raise AirflowException(
                f"{yyyymm}/{lawd_cd}: API 한 응답에 {count}/{total}건만 반환됨."
            )

    result = {"lawd": lawd_cd, "yyyymm": yyyymm, "status": status, "count": count}
    if status == "ok":
        result["raw_xml"] = base64.b64encode(raw_xml).decode("ascii")
    print(f"month={yyyymm}, lawd={lawd_cd}, status={status}, count={count}")
    return result


def collection_results(ti):
    return [ti.xcom_pull(task_ids=f"collect_group.collect_{code}") for code in DISTRICTS]


def choose_branch(ti):
    results = collection_results(ti)
    paths = []
    if any(result["status"] == "ok" for result in results):
        paths.append("upload_valid")
    if any(result["status"] != "ok" for result in results):
        paths.append("skip_upload")
    return paths


def report_skipped(ti):
    for result in collection_results(ti):
        if result["status"] != "ok":
            print(f"Skipped {result['yyyymm']}/{result['lawd']}: {result['status']}")


def upload_valid(ti):
    region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    session = boto3.Session(region_name=region)
    s3 = session.client("s3")
    configured_bucket = os.getenv("REAL_ESTATE_BUCKET", "").strip()
    owner = None if configured_bucket else session.client("sts").get_caller_identity()["Account"]
    bucket = configured_bucket or f"realestate-baenahyeon-{owner}"
    owner_check = {"ExpectedBucketOwner": owner} if owner else {}

    for result in collection_results(ti):
        if result["status"] != "ok":
            continue
        key = f"bronze/{result['yyyymm']}/{result['lawd']}.xml"
        put_args = {
            "Bucket": bucket,
            "Key": key,
            "Body": base64.b64decode(result["raw_xml"], validate=True),
            "ContentType": "application/xml",
            **owner_check,
        }
        try:
            s3.put_object(**put_args)
        except ClientError as exc:
            if configured_bucket or exc.response["Error"]["Code"] != "NoSuchBucket":
                raise
            create_args = {"Bucket": bucket}
            if region != "us-east-1":
                create_args["CreateBucketConfiguration"] = {"LocationConstraint": region}
            try:
                s3.create_bucket(**create_args)
            except ClientError as create_exc:
                if create_exc.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise
            s3.put_object(**put_args)
        print(f"s3://{bucket}/{key} ({result['count']} records)")


start_month = os.getenv("REAL_ESTATE_START_MONTH", "2026-09")
has_credentials = all(
    os.getenv(name)
    for name in ("DATA_GO_KR_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
)

with DAG(
    dag_id="bronze_realestate_collect",
    schedule="@monthly",
    start_date=pendulum.from_format(f"{start_month}-01", "YYYY-MM-DD", tz="Asia/Seoul"),
    catchup=True,
    is_paused_upon_creation=not has_credentials,
    max_active_runs=1,
    max_active_tasks=8,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
) as dag:
    with TaskGroup("collect_group") as collect_group:
        for district in DISTRICTS:
            PythonOperator(
                task_id=f"collect_{district}",
                python_callable=collect,
                op_kwargs={"lawd_cd": district},
                show_return_value_in_logs=False,
            )

    branch = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=choose_branch,
    )
    upload = PythonOperator(task_id="upload_valid", python_callable=upload_valid)
    skip = PythonOperator(task_id="skip_upload", python_callable=report_skipped)
    collect_group >> branch >> [upload, skip]
