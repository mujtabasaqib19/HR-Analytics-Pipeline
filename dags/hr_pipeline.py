from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='hr_data_etl_dag',
    default_args=default_args,
    schedule_interval=None,
    description='ETL for HR dataset into silver and gold schemas',
    tags=['hr', 'etl', 'postgres']
) as dag:

    create_silver_tables = PostgresOperator(
        task_id='create_silver_tables',
        postgres_conn_id='my_postgres_conn',
        sql="""
        CREATE SCHEMA IF NOT EXISTS silver;

        DROP TABLE IF EXISTS silver.employee_clean;
        CREATE TABLE silver.employee_clean AS
        SELECT 
            CAST(employee_id AS INT) AS employee_id,
            TRIM(first_name) AS first_name,
            TRIM(last_name) AS last_name,
            TRIM(email) AS email,
            TRIM(phone_number) AS phone_number,
            CAST(hire_date AS DATE) AS hire_date,
            CAST(department_id AS INT) AS department_id
        FROM public.employee
        WHERE employee_id IS NOT NULL;

        DROP TABLE IF EXISTS silver.salary_clean;
        CREATE TABLE silver.salary_clean AS
        SELECT 
            CAST(salary_id AS INT) AS salary_id,
            CAST(employee_id AS INT) AS employee_id,
            CAST(salary AS NUMERIC(15,2)) AS salary,
            CAST(start_date AS DATE) AS start_date,
            CAST(end_date AS DATE) AS end_date
        FROM public.salary
        WHERE employee_id IS NOT NULL;

        DROP TABLE IF EXISTS silver.job_history_clean;
        CREATE TABLE silver.job_history_clean AS
        SELECT 
            CAST(job_history_id AS INT) AS job_history_id,
            CAST(employee_id AS INT) AS employee_id,
            CAST(start_date AS DATE) AS start_date,
            CAST(end_date AS DATE) AS end_date,
            CAST(all_jobs_id AS INT) AS all_jobs_id
        FROM public.job_history
        WHERE employee_id IS NOT NULL;

        DROP TABLE IF EXISTS silver.department_clean;
        CREATE TABLE silver.department_clean AS
        SELECT 
            CAST(department_id AS INT) AS department_id,
            TRIM(department_name) AS department_name,
            CAST(current_manager_id AS INT) AS current_manager_id,
            CAST(location_id AS INT) AS location_id
        FROM public.department
        WHERE department_id IS NOT NULL;

        DROP TABLE IF EXISTS silver.department_manager_history_clean;
        CREATE TABLE silver.department_manager_history_clean AS
        SELECT 
            CAST(manager_id AS INT) AS manager_id,
            CAST(employee_id AS INT) AS employee_id,
            CAST(department_id AS INT) AS department_id,
            CAST(start_date AS DATE) AS start_date,
            CAST(end_date AS DATE) AS end_date
        FROM public.department_manager_history
        WHERE manager_id IS NOT NULL;

        DROP TABLE IF EXISTS silver.location_clean;
        CREATE TABLE silver.location_clean AS
        SELECT 
            CAST(location_id AS INT) AS location_id,
            TRIM(city) AS city,
            TRIM(state) AS state,
            TRIM(country) AS country,
            TRIM(region) AS region
        FROM public.location
        WHERE location_id IS NOT NULL;

        DROP TABLE IF EXISTS silver.positions_clean;
        CREATE TABLE silver.positions_clean AS
        SELECT 
            CAST(all_jobs_id AS INT) AS all_jobs_id,
            TRIM(job_title) AS job_title,
            CAST(min_salary AS NUMERIC(15,2)) AS min_salary,
            CAST(max_salary AS NUMERIC(15,2)) AS max_salary
        FROM public.positions
        WHERE all_jobs_id IS NOT NULL;
        """
    )


    create_gold_tables = PostgresOperator(
        task_id='create_gold_tables',
        postgres_conn_id='my_postgres_conn',
        sql="""
        CREATE SCHEMA IF NOT EXISTS gold;
        DROP TABLE IF EXISTS gold.dim_employee;
        CREATE TABLE gold.dim_employee AS
        SELECT 
            e.employee_id,
            e.first_name,
            e.last_name,
            e.email,
            e.phone_number,
            e.hire_date,
            e.department_id,
            d.department_name,
            d.current_manager_id,
            d.location_id,
            l.city AS location_city,
            l.state AS location_state,
            l.country AS location_country
        FROM silver.employee_clean e
        LEFT JOIN silver.department_clean d ON e.department_id = d.department_id
        LEFT JOIN silver.location_clean l ON d.location_id = l.location_id;

        DROP TABLE IF EXISTS gold.fact_salary;
        CREATE TABLE gold.fact_salary AS
        SELECT 
            s.salary_id,
            s.employee_id,
            d.department_name,
            s.salary,
            s.start_date,
            s.end_date
        FROM silver.salary_clean s
        LEFT JOIN silver.employee_clean e ON s.employee_id = e.employee_id
        LEFT JOIN silver.department_clean d ON e.department_id = d.department_id;

        DROP TABLE IF EXISTS gold.dim_job;
        CREATE TABLE gold.dim_job AS
        SELECT 
            p.all_jobs_id,
            p.job_title,
            p.min_salary,
            p.max_salary
        FROM silver.positions_clean p;

        DROP TABLE IF EXISTS gold.fact_job_history;
        CREATE TABLE gold.fact_job_history AS
        SELECT 
            j.job_history_id,
            j.employee_id,
            e.first_name,
            e.last_name,
            j.start_date,
            j.end_date,
            p.job_title
        FROM silver.job_history_clean j
        LEFT JOIN silver.employee_clean e ON j.employee_id = e.employee_id
        LEFT JOIN silver.positions_clean p ON j.all_jobs_id = p.all_jobs_id;

        -- Safe ALTER statement
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns 
                WHERE table_schema = 'gold'
                AND table_name = 'fact_salary'
                AND column_name = 'salary'
                AND data_type != 'integer'
            ) THEN
                EXECUTE 'ALTER TABLE gold.fact_salary ALTER COLUMN salary TYPE INTEGER USING ROUND(salary)::INTEGER';
            END IF;
        END;
        $$;
        """
    )