-- dim_customers.sql
-- Customer dimension derived from orders

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select
        customer_id,
        customer_country                    as country,
        customer_city                       as city,
        customer_age_group                  as age_group,
        customer_gender                     as gender,
        customer_loyalty_tier               as loyalty_tier,
        min(order_date)                     as first_order_date,
        max(order_date)                     as last_order_date,
        count(distinct order_id)            as total_orders,
        row_number() over (
            partition by customer_id
            order by max(order_date) desc
        ) as rn
    from orders
    group by
        customer_id,
        customer_country,
        customer_city,
        customer_age_group,
        customer_gender,
        customer_loyalty_tier
)

select
    customer_id,
    country,
    city,
    age_group,
    gender,
    loyalty_tier,
    first_order_date,
    last_order_date,
    total_orders
from customers
where rn = 1
