from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from get_query_to_db import insert_new_messages_in_db

etl_dag = DAG(
    dag_id='ETL_messages',
    default_args={
                'owner': "Jeroenzai",
                'start_date': datetime(2020, 12, 23)
                },
    schedule_interval="0 12 * * * "
)

insert_messages = PythonOperator(
    task_id="insert_messages",
    python_callable=insert_new_messages_in_db,
    dag=etl_dag
)

insert_messages