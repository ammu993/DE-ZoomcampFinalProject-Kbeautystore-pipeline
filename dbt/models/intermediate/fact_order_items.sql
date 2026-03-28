-- fact_order_items.sql
-- Core fact table: one row per line item, joined with dims

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        channel,
        order_status,
        shipping_country,
        customer_id,
        customer_loyalty_tier,
        year_week
    from {{ ref('stg_orders') }}
),

products as (
    select
        product_id,
        product_name,
        brand,
        category,
        subcategory
    from {{ ref('dim_products') }}
)

select
    -- keys
    oi.line_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,

    -- dates
    oi.order_date,
    oi.order_year,
    oi.order_month,
    oi.order_week,
    oi.year_week,

    -- order context
    o.channel,
    o.order_status,
    o.shipping_country,
    o.customer_loyalty_tier,

    -- product context
    p.product_name,
    p.brand,
    p.category,
    p.subcategory,

    -- measures
    oi.quantity,
    oi.original_price,
    oi.sale_price,
    oi.discount_pct,
    oi.cost_price,
    oi.line_total,
    oi.line_cost,
    oi.gross_profit,
    oi.is_discounted

from order_items oi
left join orders o      using (order_id)
left join products p    using (product_id)

where o.order_status = 'completed'   -- only count completed orders in facts
