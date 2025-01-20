{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_id',
        incremental_strategy = 'delete+insert'
    )
}}

with customer_interactions as (
    select
        c.customer_id,
        c.registration_date,
        d.date as interaction_date
    from {{ ref('stg_customers') }} as c
    inner join {{ ref('int_fact_customer_interaction') }} as ci
    on c.customer_id = ci.customer_id
    inner join {{ ref('stg_dates') }} as d
    on ci.interaction_date_id = d.date_id
)

select *
from customer_interactions
where interaction_date < registration_date