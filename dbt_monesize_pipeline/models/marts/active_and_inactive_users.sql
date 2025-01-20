{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_id',
        incremental_strategy = 'delete+insert'
    )
}}


with source_data as (
    select
        c.customer_id,
        c.company_name,
        c.subscription_plan,
        count(distinct s.interaction_id) as total_interactions,
        max(d.date) as last_interaction,
        min(d.date) as first_interaction,
        datediff('day', max(d.date), '2024-12-31') as days_since_last_activity,
        case
            when datediff('day', max(d.date), '2024-12-31') <= 30 then 'Active'
            else 'Inactive'
        end as activity
    from {{ ref('int_fact_customer_interaction') }} as s
    inner join {{ ref('stg_dates') }} as d
        on s.interaction_date_id = d.date_id
    inner join {{ ref('stg_customers') }} as c
        on s.customer_id = c.customer_id
    group by c.customer_id, c.subscription_plan, c.company_name
)
select
    customer_id,
    company_name,
    subscription_plan,
    total_interactions,
    last_interaction,
    first_interaction,
    days_since_last_activity,
    activity
from source_data

