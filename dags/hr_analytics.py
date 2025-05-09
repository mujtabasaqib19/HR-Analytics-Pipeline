from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='hr_analytics_dag',
    default_args=default_args,
    schedule_interval=None,
    description='Run analytics on gold layer HR tables',
    tags=['analytics', 'hr', 'postgres']
) as dag:

    avg_salary_by_department = PostgresOperator(
        task_id='avg_salary_by_department',
        postgres_conn_id='my_postgres_conn',
        sql="""
        SELECT 
            department_name,
            ROUND(AVG(salary), 2) AS avg_salary
        FROM gold.fact_salary
        GROUP BY department_name
        ORDER BY avg_salary DESC;
        """
    )

    promotion_count = PostgresOperator(
        task_id='promotion_count',
        postgres_conn_id='my_postgres_conn',
        sql="""
        SELECT 
            employee_id,
            COUNT(*) AS num_promotions
        FROM gold.fact_job_history
        GROUP BY employee_id
        HAVING COUNT(*) > 1
        ORDER BY num_promotions DESC;
        """
    )

    employees_per_job_title = PostgresOperator(
        task_id='employees_per_job_title',
        postgres_conn_id='my_postgres_conn',
        sql="""
        SELECT 
            job_title,
            COUNT(DISTINCT employee_id) AS num_employees
        FROM gold.fact_job_history
        GROUP BY job_title
        ORDER BY num_employees DESC;
        """
    )