{{ config(
    materialized = 'table',
    schema = 'STAGE',
    database = 'AIRFLOW_DB'
) }}

select * from {{source('trialsource','CUSTOMERS_SCD_TEST')}}