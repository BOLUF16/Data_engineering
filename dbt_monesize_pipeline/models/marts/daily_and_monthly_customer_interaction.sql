{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_id',
        incremental_strategy = 'delete+insert'
    )
}}


SELECT 
    d.date,
    d.month, 
    d.year, 
    count(f.interaction_id) as daily_interactions,
    sum(count(f.interaction_id)) over (partition by d.month, d.year) as monthly_interactions
from {{ref('int_fact_customer_interaction')}} as f
inner join {{ref('stg_dates')}} as d on f.interaction_date_id = d.date_id
group by 
    d.date, d.month, d.year
order by 
    d.year, d.month, d.date