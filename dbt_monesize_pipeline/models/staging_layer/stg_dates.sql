{{
    config(
        materialized = 'incremental',
        unique_key = 'date_id',
        incremental_strategy = 'delete+insert'
    )
}}


with dates as(
    select 
        date_id,
        date,
        day,
        month,
        year,
        weekday,
        getdate() as loaded_at
     
    from {{ source('monesize', 'dim_date')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}

)

select * from dates