-- stg_orders.sql
-- Cleans and casts the raw orders staging table from Spark

with source as (
    select * from {{ source('kbeauty_pipeline', 'stg_orders') }}
),

cleaned as (
    select
        order_id,
        order_timestamp,
        cast(order_date as date)                    as order_date,
        lower(trim(channel))                        as channel,
        lower(trim(order_status))                   as order_status,
        upper(trim(shipping_country))               as shipping_country,
        customer_id,
        customer_country,
        customer_city,
        customer_age_group,
        customer_gender,
        customer_loyalty_tier,
        _ingestion_date,
        _ingested_at,

        -- derived
        extract(year from cast(order_date as date))         as order_year,
        extract(month from cast(order_date as date))        as order_month,
        extract(week from cast(order_date as date))         as order_week,
        format_date('%Y-W%W', cast(order_date as date))     as year_week

    from source
    where order_id is not null
)

select * from cleaned
