import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

OWNER_EMAIL = "mhirose@mozilla.com"

# Deploy value associated with Microsoft Store keys in k8s secret `airflow-gke-secrets` in environments Microsoft variables.

microsoft_client_id = Secret(
    deploy_type="env",
    deploy_target="MICROSOFT_CLIENT_ID",
    secret="airflow-gke-secrets",
    key="MICROSOFT_CLIENT_ID",
)
microsoft_client_secret = Secret(
    deploy_type="env",
    deploy_target="MICROSOFT_CLIENT_SECRET",
    secret="airflow-gke-secrets",
    key="MICROSOFT_CLIENT_SECRET",
)
microsoft_tenant_id = Secret(
    deploy_type="env",
    deploy_target="MICROSOFT_TENANT_ID",
    secret="airflow-gke-secrets",
    key="MICROSOFT_TENANT_ID",
)
microsoft_store_app_list = Secret(
    deploy_type="env",
    deploy_target="MICROSOFT_STORE_APP_LIST",
    secret="airflow-gke-secrets",
    key="MICROSOFT_STORE_APP_LIST",
)

docs = f"""
This DAG runs the daily download of aggregated data from the Microsoft Store API.
#### Owner
{OWNER_EMAIL}
#### Tags
* impact/tier_2
* repo/bigquery-etl
"""

default_args = {
    "owner": OWNER_EMAIL,
    "start_date": datetime.datetime(2024, 6, 18, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", OWNER_EMAIL],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = [Tag.ImpactTier.tier_2]

# Have the DAG run later in the day so Microsoft Store data has a chance to populate
with DAG(
    "microsoft_store",
    default_args=default_args,
    schedule_interval="0 15 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    table_names = (
        "app_acquisitions",
        "app_conversions",
        "app_installs",
    )
    for table in table_names:
        GKEPodOperator(
            task_id=f"microsoft_derived__{table}__v1",
            secrets=[
                microsoft_client_id,
                microsoft_client_secret,
                microsoft_tenant_id,
                microsoft_store_app_list,
            ],
            arguments=[
                "python",
                f"sql/moz-fx-data-shared-prod/microsoft_derived/{table}_v1/query.py",
                "--date={{ macros.ds_add(ds, -3) }}",
            ],
            image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
            owner=OWNER_EMAIL,
            email=[OWNER_EMAIL, "telemetry-alerts@mozilla.com"],
        )
