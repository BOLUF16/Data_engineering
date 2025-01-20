{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_id',
        incremental_strategy = 'delete+insert'
    )
}}


with customer_interactions as (
    select
        ci.interaction_id,
        ci.customer_id,
        ci.location_id,
        ci.interaction_type,
        ci.interaction_date_id,
        ci.channels,
        ci.duration_minutes,
        ci.action_taken,
        ci.associated_product,
        ci.features_used,
        ci.outcome,
        c.customer_id as customers_id,
        c.registration_date,
        d.date as interaction_date
    from {{ ref('stg_customers') }} as c
    inner join {{ ref('stg_fact_customer_interaction') }} as ci
    on c.customer_id = ci.customer_id
    inner join {{ ref('stg_dates') }} as d
    on ci.interaction_date_id = d.date_id
)

select
    interaction_id,
    customer_id,
    location_id,
    interaction_type,
    interaction_date_id,
    channels,
    duration_minutes,
    action_taken,
    associated_product,
    features_used,
    outcome
from customer_interactions
where interaction_date >= registration_date