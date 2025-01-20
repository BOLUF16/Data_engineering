{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_id',
        incremental_strategy = 'delete+insert'
    )
}}


with source_data as (
    select
        d.year,
        d.month,
        c.subscription_plan,
        s.customer_id,
        max(d.date) as last_interaction_date 
    from {{ ref('int_fact_customer_interaction') }} as s
    inner join {{ ref('stg_dates') }} as d
        on s.interaction_date_id = d.date_id
    inner join {{ ref('stg_customers') }} as c
        on s.customer_id = c.customer_id
    group by 
        d.year, d.month, c.subscription_plan, s.customer_id
),
active_users as (
    select
        year,
        month,
        subscription_plan,
        customer_id,
        last_interaction_date,
        date_trunc('month', last_interaction_date) + interval '1 month' - interval '1 day' as end_of_month
    from source_data
),
filtered_active_users as (
    select
        year,
        month,
        subscription_plan,
        count(distinct customer_id) as total_active_users
    from active_users
    where datediff('day', last_interaction_date, end_of_month) <= 30
    group by 
        year, month, subscription_plan
)
select
    year,
    month,
    subscription_plan,
    total_active_users
from filtered_active_users
order by year, month, subscription_plan

