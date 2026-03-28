-- dim_products.sql
-- Product dimension table

with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    brand,
    category,
    subcategory,
    sku,
    unit_price,
    cost_price,
    round(unit_price - cost_price, 2)       as unit_margin,
    round((unit_price - cost_price)
          / nullif(unit_price, 0) * 100, 2) as margin_pct,
    is_active,
    currency,
    snapshot_date                           as last_updated
from products
