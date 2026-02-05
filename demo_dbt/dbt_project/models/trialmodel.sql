/*{{ config(
    materialized = 'table',
    schema = 'DBT_SDUNG',
    database = 'AIRFLOW_DB'
) }}

select * from {{source('trialsource','CUSTOMERS_SCD_TEST')}}*/

{{ config(
    materialized = 'table',
    schema = 'DBT_SDUNG',
    database = 'AIRFLOW_DB'
) }}

{% set selected_customer = var('selected_customer', 'all') %}

-- Simple model with 1 flag demonstrated

with source_data as (
    select * 
    from {{ source('trialsource','CUSTOMERS_SCD_TEST') }}
),

filtered_data as (
    select *
    from source_data
    where 1=1
    {{ build_email_filter(selected_customer) }}
)

select *
from filtered_data
    