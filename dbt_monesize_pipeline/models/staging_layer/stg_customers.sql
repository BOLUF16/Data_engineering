{{
    config(
        materialized = 'incremental',
        unique_key = 'customer_id',
        merge_update_columns = ['company_size', 'subscription_plan'],
        incremental_strategy = 'merge'
    )
}}


with customers as(
    select 
        customer_id,
        company_name,
        case
            when company_size = 'Medium size' then 'Medium'
            else 'Small'
            end as company_size,
            
        email,
        industry,
        phone_number,
        registration_date,
        subscription_plan,
        getdate() as loaded_at
     
    from {{ source('monesize', 'dim_customer')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}

)

select * from customers