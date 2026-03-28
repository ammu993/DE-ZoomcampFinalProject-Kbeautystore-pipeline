-- mart_top_products.sql
-- Dashboard Tile 2: Top products by units sold per week
-- Powers: leaderboard/bar chart of best-selling products

with fact as (
    select * from {{ ref('fact_order_items') }}
),

weekly_product as (
    select
        year_week,
        order_year,
        order_week,
        date_trunc(order_date, week(monday))    as week_start_date,
        product_id,
        product_name,
        brand,
        category,
        subcategory,

        sum(quantity)                           as units_sold,
        round(sum(line_total), 2)               as revenue,
        round(sum(gross_profit), 2)             as gross_profit,
        count(distinct order_id)                as orders_containing_product,
        round(avg(discount_pct) * 100, 1)       as avg_discount_pct

    from fact
    group by
        year_week,
        order_year,
        order_week,
        week_start_date,
        product_id,
        product_name,
        brand,
        category,
        subcategory
),

ranked as (
    select
        *,
        row_number() over (
            partition by year_week
            order by units_sold desc
        ) as rank_by_units,

        row_number() over (
            partition by year_week
            order by revenue desc
        ) as rank_by_revenue

    from weekly_product
)

select * from ranked
order by week_start_date desc, rank_by_units
