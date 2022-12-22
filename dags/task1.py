from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='task_1',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule='@once',
    dagrun_timeout=timedelta(minutes=60),
    # description='use case of psql operator in airflow',
    # start_date=airflow.utils.dates.days_ago(1)
)

cust = """
DROP TABLE IF EXISTS public.customer;
CREATE TABLE public.customer AS
SELECT DISTINCT customer_id
,customer_name
,address
,city_id
FROM public.raw_customer_data;
"""

order_dim = """
INSERT INTO public.order_dim AS
SELECT DISTINCT order_id
,order_date
,album_id
,customer_id
,employee_id
,now() as ingesttime
FROM public.raw_order_data
WHERE order_date > (SELECT MAX(order_date) FROM public.order_dim);
"""

album_dim = """
DROP TABLE IF EXISTS public.album_dim;
CREATE TABLE public.album_dim AS
SELECT DISTINCT album_id
,album_name
,artist_id
,album_price
FROM public.raw_album_data;
"""

artist_dim = """
DROP TABLE IF EXISTS public.artist_dim;
CREATE TABLE public.artist_dim AS
SELECT DISTINCT artist_id
,artist_name
,status
FROM public.raw_album_data;
"""

emp_dim = """
DROP TABLE IF EXISTS public.emp_dim;
CREATE TABLE public.emp_dim AS
SELECT DISTINCT employee_id
,employee_name
,title
,department
,region_id
,salary
FROM public.raw_office_data;
"""

address_dim = """
DROP TABLE IF EXISTS public.add_dim;
CREATE TABLE public.add_dim AS
SELECT city_id
,city_name
,region_id
,country_code
,country_name
FROM public.raw_regional_data;
"""

fact_sales = """
DROP TABLE IF EXISTS public.fact_sales;
CREATE TABLE public.fact_sales AS
WITH ord AS (
    SELECT album_id
    ,COUNT(DISTINCT order_id) as total_order
    FROM public.order_dim
    GROUP BY album_id
)
SELECT a.album_name
,ar.artist_name
,c.customer_name
,e.employee_name
,o.total_order
,o.total_order * album_price as total_sales_revenue
,now() as ingesttime
FROM public.album_dim a
LEFT JOIN public.artist_dim ar ON a.album_id = ar.album_id
LEFT JOIN ord o ON a.album_id = o.album_id
LEFT JOIN public.order_dim od ON a.album_id = or.album_id
LEFT JOIN public.customer_id c ON or.customer_id = c.customer_id
LEFT JOIN public.emp_dim e ON or.employee_id = e.employee_id;
"""

cust_table = PostgresOperator(
    sql=cust,
    task_id="create_cust_dim_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

order_table = PostgresOperator(
    sql=order_dim,
    task_id="insert_order_dim_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

album_table = PostgresOperator(
    sql=album_dim,
    task_id="create_album_dim_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

artist_table = PostgresOperator(
    sql=artist_dim,
    task_id="create_artist_dim_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

emp_table = PostgresOperator(
    sql=emp_dim,
    task_id="create_emp_dim_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

add_table = PostgresOperator(
    sql=address_dim,
    task_id="create_address_dim_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

fact_table = PostgresOperator(
    sql=fact_sales,
    task_id="create_fact_sales_table",
    postgres_conn_id="postgres_default",
    dag=dag
)

[cust_table, order_table, album_table, artist_table,
    emp_table, add_table] >> fact_table

if __name__ == "__main__":
    dag.cli()
