import operator
import re
from functools import partial
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import define_temp_db_schema, pg_params
from common.sql import SQL_DROP_TABLE
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.precariobelasting_add import ADD_GEBIED_COLUMN, ADD_TITLE, RENAME_DATAVALUE_GEBIED
from swift_operator import SwiftOperator

DAG_ID: Final = "rioolnetwerk"
variables: dict = Variable.get(DAG_ID, deserialize_json=True)
tmp_dir: str = f"{SHARED_DIR}/{DAG_ID}"
tmp_database_schema: str = define_temp_db_schema(dataset_name=DAG_ID)
data_endpoints: dict[str, str] = variables["temp_data"]
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}

# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=DAG_ID)
