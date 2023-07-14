try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email_operator import EmailOperator
    from datetime import datetime
    import pandas as pd

    #Setting up Triggers
    from airflow.utils.trigger_rule import TriggerRule
    
    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("first_function_execute   ")
    context['ti'].xcom_push(key='mykey', value="first_function_execute says Hello ")


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name":"Lovepreet","title":"Software Engineer"}, { "name":"Aman","title":"Business Analyst"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print(df.head())
    print('@'*66)

    print("I am in second_function_execute got value :{} from Function 1  ".format(instance))

def on_failure_callback(context):
    print("Fail works  !  ")

with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2023, 7, 4),
            'on_failure_callback': on_failure_callback,
            'email': ['lovepreet.multani20@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
        },
        catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Lovepreet Multani"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )

    email = EmailOperator(
        task_id='send_email',
        to='lovepreet.multani20@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test Airflow </h3> """,
    )

first_function_execute >> second_function_execute >> email