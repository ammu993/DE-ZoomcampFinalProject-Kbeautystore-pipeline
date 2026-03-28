-- stg_order_items.sql
-- Cleans and casts the raw order items staging table from Spark

with source as (
    select * from {{ source('kbeauty_pipeline', 'stg_order_items') }}
),

cleaned as (
    select
        line_item_id,
        order_id,
        cast(order_date as date)            as order_date,
        product_id,
        product_name,
        brand,
        category,
        subcategory,
        cast(quantity as int64)             as quantity,
        cast(original_price as float64)     as original_price,
        cast(sale_price as float64)         as sale_price,
        cast(discount_pct as float64)       as discount_pct,
        cast(cost_price as float64)         as cost_price,
        cast(line_total as float64)         as line_total,
        cast(line_cost as float64)          as line_cost,
        _ingestion_date,
        _ingested_at,

        -- derived
        round(line_total - line_cost, 2)    as gross_profit,
        case
            when discount_pct > 0 then true
            else false
        end                                 as is_discounted,
        extract(year from cast(order_date as date))         as order_year,
        extract(month from cast(order_date as date))        as order_month,
        extract(week from cast(order_date as date))         as order_week,
        format_date('%Y-W%W', cast(order_date as date))     as year_week

    from source
    where line_item_id is not null
      and sale_price > 0
      and quantity > 0
)

select * from cleaned
