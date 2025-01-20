select
    *
from {{ref('stg_customers')}} as c
inner join {{ref('int_fact_customer_interaction')}} as ci
on c.customer_id = ci.customer_id
inner join {{ref('stg_dates')}} as d
on ci.interaction_date_id = d.date_id
where d.date < c.registration_date