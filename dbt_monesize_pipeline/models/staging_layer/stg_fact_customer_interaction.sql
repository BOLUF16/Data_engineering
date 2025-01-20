{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_id',
        incremental_strategy = 'delete+insert'
    )
}}


with fact_interaction as(
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
        outcome,
        getdate() as loaded_at
     
    from {{ source('monesize', 'fact_interaction')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}

)

select * from fact_interaction