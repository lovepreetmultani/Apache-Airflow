import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_movie_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_db',schema='test',)
    print(pg_hook)
    conn=pg_hook.get_conn()
    print(conn)
    cursor=conn.cursor()
    sql = "SELECT * FROM movies"
    cursor.execute(sql)
    with open(f"dags/get_movies.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()

with DAG(
        dag_id="postgres_db_dag",
        schedule_interval="@daily",
        start_date= datetime(year=2023, month=7, day=24),
        catchup=False
) as dag:
    task_get_movie_data=PythonOperator(
        task_id='get_movie_data',
        python_callable=get_movie_data,
        do_xcom_push=False
    )

task_get_movie_data