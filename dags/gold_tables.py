from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def transfer_table_to_snowflake(pg_table, sf_table, limit_rows=None):
    def _transfer():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        sf_hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')

        query = f"SELECT * FROM gold.{pg_table}"
        if limit_rows:
            query += f" LIMIT {limit_rows}"

        records = pg_hook.get_records(query)
        if not records:
            raise ValueError(f"No data found in table: {pg_table}")

        chunk_size = 1000
        total = len(records)
        print(f"Transferring {total} rows from {pg_table} to Snowflake table {sf_table}...")

        for i in range(0, total, chunk_size):
            chunk = records[i:i+chunk_size]
            sf_hook.insert_rows(
                table=sf_table,
                rows=chunk,
                target_fields=None,
                commit_every=chunk_size
            )
            print(f"Inserted rows {i + 1} to {i + len(chunk)}")

        print(f"✅ Transfer complete for {pg_table}")
    return _transfer

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='transfer_gold_to_snowflake',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'snowflake']
) as dag:

    transfer_dim_employee = PythonOperator(
        task_id='transfer_dim_employee',
        python_callable=transfer_table_to_snowflake('dim_employee', 'DIM_EMPLOYEE', limit_rows=2500)
    )

    transfer_fact_salary = PythonOperator(
        task_id='transfer_fact_salary',
        python_callable=transfer_table_to_snowflake('fact_salary', 'FACT_SALARY', limit_rows=2500)
    )

    transfer_dim_job = PythonOperator(
        task_id='transfer_dim_job',
        python_callable=transfer_table_to_snowflake('dim_job', 'DIM_JOB')
    )

    transfer_fact_job_history = PythonOperator(
        task_id='transfer_fact_job_history',
        python_callable=transfer_table_to_snowflake('fact_job_history', 'FACT_JOB_HISTORY', limit_rows=2500)
    )

    [transfer_dim_employee, transfer_fact_salary, transfer_dim_job] >> transfer_fact_job_history
