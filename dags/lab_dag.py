import os
import time
import json
from datetime import datetime
from pathlib import Path

import snowflake.connector
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Set your name to personalize table name
NAME = "melanie"  # TODO 0: input your name to differentiate your table from others'
TABLE_NAME = f"airflow_lab_{NAME}"


def get_snowflake_connection():
    # TODO 1: Specify Airflow variables. Import them here.
    return snowflake.connector.connect(
        user=Variable.get("snowflake_user", default_var="MELANIEYES"),
        password=Variable.get("snowflake_password", default_var="BuiCaoDongNghi@1"),
        account=Variable.get("snowflake_account", default_var="TJZIEET-CR93617"),
        database="DB_LAB_M1W4",
        schema="SC_LAB_M1W4_DEMO",
    )


def get_snowflake_hook():
    # TODO 2: specify Snowflake connection
    return SnowflakeHook(snowflake_conn_id="snowflake_academy")
    

@task()
def create_table():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} (id STRING, data INT)")
    conn.commit()
    cur.close()
    conn.close()


@task()
def check_for_json_data() -> list[str]:
    """
    # TODO 3: implement sensor logic
    Wait for JSON data to appear using polling mechanism.
    """
    timeout = 120
    interval = 15
    waited = 0

    airflow_root = Path("/opt/airflow")
    print(f"Scanning for JSON files from: {airflow_root}")

    while waited < timeout:
        json_files = list(airflow_root.glob("data*.json"))
        if json_files:
            return [str(file.resolve()) for file in json_files]
        time.sleep(interval)
        waited += interval

    print(f"No JSON files found in: {airflow_root}")
    raise FileNotFoundError("No JSON files found")


@task()
def process_files(file_paths: list[str]) -> bool:
    if not file_paths:
        print("No JSON files were provided to process.")
        return True  # Still mark the task as successful

    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"Skipping: File does not exist -> {file_path}")
            continue

        try:
            with open(file_path, "r") as f:
                row_data = json.load(f)

            id_val = row_data.get("id")
            data_val = row_data.get("data")

            if id_val is None or data_val is None:
                print(f"Missing 'id' or 'data' in file: {file_path}")
                continue

            hook = get_snowflake_hook()
            hook.run(f"INSERT INTO {TABLE_NAME} VALUES ('{id_val}', {data_val})")
            print(f"Inserted into Snowflake: ID={id_val}, Data={data_val}")

        except Exception as e:
            print(f"Failed to process {file_path}: {e}")
            continue

    return True


@task(trigger_rule="all_done")
def dag_success_notification():
    # Try to get the webhook; fallback to None if not found
    discord_webhook = Variable.get("discord_webhook", default_var=None)

    if not discord_webhook:
        print("Discord webhook not set in Airflow Variables. Skipping notification.")
        return

    message = f"DAG airflow_lab for {NAME} completed successfully at {datetime.now()}"
    try:
        os.system(
            f"curl -H 'Content-Type: application/json' -X POST -d '{{\"content\": \"{message}\"}}' {discord_webhook}"
        )
        print("Notification sent to Discord.")
    except Exception as e:
        print(f"Failed to send Discord notification: {e}")


def discord_on_dag_failure_callback(context):
    discord_webhook = Variable.get("discord_webhook")
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    execution_date = context["execution_date"]
    message = f"DAG {dag_id} for {NAME} failed at task {task_id} on {execution_date}"
    os.system(
        f"curl -H 'Content-Type: application/json' -X POST -d '{{\"content\": \"{message}\"}}' {discord_webhook}"
    )


@dag(
    dag_id="airflow_lab",
    schedule=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    on_failure_callback=discord_on_dag_failure_callback,
)
def airflow_lab():
    """
    # TODO 5: Connect all the tasks
    """
    create_table_task = create_table()
    json_files = check_for_json_data()
    process_task = process_files(json_files)
    notification_task = dag_success_notification()

    create_table_task >> json_files >> process_task >> notification_task


airflow_lab()
