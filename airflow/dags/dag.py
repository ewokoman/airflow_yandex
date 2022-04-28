from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests



def extract_data(url, **kwargs):
    """ Extract DATA
    """
    response = requests.get(url)
    data = response.json()
    return_value = data['rates']['USD']
    kwargs['ti'].xcom_push(key='return_value',value=return_value)



with DAG(dag_id='data',
         default_args={'owner': 'airflow'},
         schedule_interval='0 */3 * * *',
         start_date=days_ago(0)
    ) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        op_kwargs={
            'url': 'https://api.exchangerate.host/latest?base=BTC&symbols=USD'},
        dag=dag
    )

    create_table = PostgresOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS btc_usd_rate(
            currency_pair VARCHAR NOT NULL,
            datetime VARCHAR NOT NULL,
            exchange_rate FLOAT NOT NULL);
          """,
    )
    
    insert_data = PostgresOperator(
        task_id="insert_data",
        sql="""
            INSERT INTO btc_usd_rate (currency_pair, datetime, exchange_rate)
            VALUES ( 'BTC_USD', '{{ ds }}', '{{ task_instance.xcom_pull(key='return_value',task_ids='extract_data') }}');
          """,
    )
    
    
    
    
    
extract_data >> create_table >> insert_data
