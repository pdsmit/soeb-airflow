import requests
import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import SHARED_DIR, default_args, MessageOperator, quote_string
from common.db import DatabaseEngine
from contact_point.callbacks import get_contact_point_on_failure_callback
from pathlib import Path
from common.path import mk_dir
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator


dag_id = "Rioolnetwerk"
tmp_dir = f"{SHARED_DIR}/{dag_id}"
db_conn: DatabaseEngine = DatabaseEngine()

variables: dict = Variable.get(DAG_ID, deserialize_json=True)
data_endpoints: dict[str, str] = variables["temp_data"]

with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    description="Rioolnetwerken aangeleverd door Waternet",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
    ) as dag:

    # 1. Post info message on slack
    slack_bot = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    make_temp_dir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file_name}",
            swift_conn_id="objectstore-waternet", # laatste 2 namen van key-vault-string gebruiken (airflow-connections-objectstore-waternet)
            container="production", # map in de objectstore
            object_id=url,
            output_path=f"{tmp_dir}/{url}",
        )
        for file_name, url in data_endpoints.items()
    ]

     
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
  slack_bot >> make_temp_dir >> download_data
)

dag.doc_md = """
    #### DAG summary
    This DAG contains Rioolnetwerken from Waternet
    """