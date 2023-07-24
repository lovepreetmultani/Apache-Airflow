import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_movie_data():
    pg_hook:PostgresHook(postgres_conn_id='postgres_db',schema='db_test',)
    print(pg_hook)
    conn=pg_hook.get_conn()
    print(conn)
    cursor=conn.cursor()
    sql = "SELECT * FROM movies"
    df_especie = pg_hook.get_pandas_df(sql)
    print(df_especie)
    tbl_dict = df_especie.to_dict('dict')
    return tbl_dict
"""     cursor.execute("SELECT * from movies")
    with open(f"dags/get_movies.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    return cursor.fetchall() """


def get_src_tables():
    pg_hook = PostgresHook(postgre_conn_id="propesq",schema="sch01",)
    print(pg_hook)
    connection = pg_hook.get_conn()
    print(connection)
    cursor = connection.cursor()
    sql = "SELECT * FROM especie"
    df_especie = pg_hook.get_pandas_df(sql)
    print(df_especie)
    tbl_dict = df_especie.to_dict('dict')
    return tbl_dict

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