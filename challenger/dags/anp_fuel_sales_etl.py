from airflow.models import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python_operator import PythonOperator

import psycopg2

import psycopg2.extras as extras

import pandas as pd

from datetime import datetime


default_args = {
        'start_date': datetime(2022, 5, 19)
}

conn = psycopg2.connect(
    database="raizen_analytics", user='airflow', password='airflow', host='postgres', port='5432'
)

df_DPCache_m3 = pd.read_excel('./dags/source/vendas-combustiveis-m3.xls', 'DPCache_m3')

df_DPCache_m3_2 = pd.read_excel('./dags/source/vendas-combustiveis-m3.xls', 'DPCache_m3_2')

cols = "combustivel,ano,regiao,estado,jan,fev,mar,abr,mai,jun,jul,ago,set,out,nov,dez,total"

def execute_values(conn, df, table_name, cols):
    
    tuples = [tuple(x) for x in df.to_numpy()]
    query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

with DAG('anp_fuel_sales_etl', 
          default_args=default_args,
          catchup =False) as dag:
    
    # create_sales_of_oil_derivative_fuels = PostgresOperator(
    #     task_id='create_sales_of_oil_derivative_fuels',
    #     postgres_conn_id="postgres",
    #     sql="sql/create_sales_of_diesel.sql",
    #     )

    # create_sales_of_diesel = PostgresOperator(
    #     task_id='create_sales_of_diesel',
    #     postgres_conn_id="postgres",
    #     sql="sql/create_sales_of_diesel.sql",
    #     )

    # create_sales_of_oil_derivative_fuels = PostgresOperator(
    #     task_id='create_sales_of_oil_derivative_fuels',
    #     postgres_conn_id="postgres",
    #     sql="sql/create_sales_of_oil_derivative_fuels.sql",
    #     )

    # create_sales_of_diesel = PostgresOperator(
    #     task_id='create_sales_of_diesel',
    #     postgres_conn_id="postgres",
    #     sql="sql/create_sales_of_diesel.sql",
    #     )

    insert_sales_of_oil_derivative_fuels = PythonOperator(
        task_id='insert_sales_of_oil_derivative_fuels',
        python_callable= execute_values,
        op_kwargs={"conn":conn, "df":df_DPCache_m3, "table_name":'sales_of_oil_derivative_fuels',"cols":cols}
    )
    
    insert_sales_of_diesel  = PythonOperator(
        task_id='insert_sales_of_diesel',
        python_callable= execute_values,
        op_kwargs={"conn":conn, "df":df_DPCache_m3_2, "table_name":'sales_of_diesel',"cols":cols}
    )

    insert_raw_sales_of_oil_derivative_fuels = PostgresOperator(
        task_id='insert_raw_sales_of_oil_derivative_fuels',
        postgres_conn_id="postgres",
        sql="./sql/insert_raw_sales_of_oil_derivative_fuels.sql",
        )

    insert_raw_sales_of_diesel = PostgresOperator(
        task_id='insert_raw_sales_of_diesel',
        postgres_conn_id="postgres",
        sql="./sql/insert_raw_sales_of_diesel.sql",
        )

    #create_sales_of_oil_derivative_fuels >> create_raw_sales_of_diesel >>
    insert_sales_of_oil_derivative_fuels  >> insert_raw_sales_of_oil_derivative_fuels >> insert_sales_of_diesel >> insert_raw_sales_of_diesel