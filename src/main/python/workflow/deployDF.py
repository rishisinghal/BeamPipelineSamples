import datetime
import uuid
import copy

from airflow import models
from airflow.contrib.operators import dataflow_operator 


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {'start_date': yesterday, 'dataflow_default_options':{'project': 'training-sandbox-sgp'}}

with models.DAG('quickstart',schedule_interval=datetime.timedelta(days=1),default_args=default_dag_args) as dagMydag:

    t1 = dataflow_operator.DataFlowJavaOperator(
               task_id='dataflow_example',
               jar='gs://ristemp/samples-0.0.1-SNAPSHOT.jar',
               gcp_conn_id='google_cloud_default',
               dag=dagMydag
)