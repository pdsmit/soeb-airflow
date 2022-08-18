import requests
import operator

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common.db import DatabaseEngine
from pathlib import Path
from common.path import mk_dir
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

team_name = "SOEB"
workload_name = "Rioolnetwerk"
dag_id = team_name + "_" + workload_name
db_conn: DatabaseEngine = DatabaseEngine()
password: str = env("objectstore_CONN_waternet_PASSWD")
user: str = env("objectstore_CONN_waternet_USER")
base_url: str = URL(env("Objectstore_CONN_BASE_URL"))

# iets wnr geen data ofzo
class DataSourceError(Exception):
    """Custom exeception for not available data source."""

    pass


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    description="Rioolnetwerken aangeleverd door Waternet",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
    ) as dag:

    # 1. Post info message on slack
    task1 = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    task2 = mk_dir(Path(tmp_dir))

    # 3. Download data
    task3 = PythonOperator(task_id="download_data", python_callable=get_data)

     
    # 4. Import .gpkg to Postgresql
    task4 = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{dag_id}_{dag_id}_new",
        input_file=f"{tmp_dir}/{dag_id}.gpkg",
        s_srs="EPSG:28992",
        t_srs="EPSG:28992",
        auto_detect_type="YES",
        geometry_name="geometry",
        fid="fid",
        mode="PostgreSQL",
        db_conn=db_conn        
    )
(
  task1 >> task2 >> task3 >> task4    
)

dag.doc_md = """
    #### DAG summary
    This DAG contains Rioolnetwerken from Waternet
    """