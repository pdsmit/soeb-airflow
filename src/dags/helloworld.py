from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from common import default_args, MessageOperator

team_name = "dave"
workload_name = "example-normal-dag"
dag_id = team_name + "_" + workload_name


def print_hello():
    return "Hello world from first Airflow DAG!"


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    description="Hello World DAG",
    schedule_interval="0 12 * * *",
) as dag:
    task1 = MessageOperator(
        task_id="slack_at_start",
    )

    task2 = PythonOperator(task_id="hello_task", python_callable=print_hello)

(task1 >> task2)
