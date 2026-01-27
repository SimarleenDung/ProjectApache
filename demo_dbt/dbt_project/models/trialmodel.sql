{{ config(
    materialized = 'table',
    schema = 'DBT_SDUNG',
    database = 'AIRFLOW_DB'
) }}

select * from {{source('trialsource','CUSTOMERS_SCD_TEST')}}