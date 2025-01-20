{{
    config(
        materialized = 'incremental',
        unique_key = 'location_id',
        incremental_strategy = 'delete+insert'
    )
}}


with locations as(
    select 
        location_id,
        address,
        city,
        state,
        country,
        getdate() as loaded_at
     
    from {{ source('monesize', 'dim_location')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}

)

select * from locations