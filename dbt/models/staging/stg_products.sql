-- stg_products.sql
-- Latest product snapshot — one row per product

with source as (
    select * from {{ source('kbeauty_pipeline', 'stg_products') }}
),

latest as (
    -- keep only the most recent snapshot per product
    select *,
        row_number() over (
            partition by product_id
            order by snapshot_date desc
        ) as rn
    from source
),

cleaned as (
    select
        product_id,
        product_name,
        brand,
        category,
        subcategory,
        cast(unit_price as float64)     as unit_price,
        cast(cost_price as float64)     as cost_price,
        sku,
        is_active,
        currency,
        snapshot_date
    from latest
    where rn = 1
      and product_id is not null
)

select * from cleaned
