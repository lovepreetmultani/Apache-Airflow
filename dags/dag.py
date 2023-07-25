try:
    from airflow import DAG
    from datetime import timedelta,datetime
    from airflow.operators.python_operator import PythonOperator
    from transformations import *

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

#defina etl function
def etl():
    movies_df=extract_movies_to_df()
    users_df=extract_users_to_df()
    transformed_df=transform_avg_ratings(movies_df,users_df)
    load_to_db(transformed_df)

#failed message
def on_failure_callback(context):
    print("Fail works  !  ")

#default arguments
default_args = {
    'owner': 'lovepreet',
    'depends_on_past': False,
    'start_date': datetime(year=2023, month=7, day=24),
    'depends_on_past': True,
    'email':['test@gmail.com'],
    'on_failure_callback': on_failure_callback,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

#instantiate DAG
with DAG(dag_id='etl_pipeline',
         default_args=default_args,
          schedule_interval="@daily",
catchup=False) as f:

    etl_task=PythonOperator(task_id='etl_task',
                        python_callable=etl,
                        provide_context=True)

etl_task