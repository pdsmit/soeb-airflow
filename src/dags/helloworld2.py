from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from common import default_args, MessageOperator

team_name = "SOEB"
workload_name = "hello_world_v2"
dag_id = team_name + "_" + workload_name


def print_hello():
    return "Hello world from first Airflow DAG! v2"


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    description="Hello World DAG v2",
    schedule_interval="0 12 * * *",
) as dag:
    task1 = MessageOperator(
        task_id="slack_at_start",
    )

    task2 = PythonOperator(task_id="hello_task", python_callable=print_hello)

(task1 >> task2)
