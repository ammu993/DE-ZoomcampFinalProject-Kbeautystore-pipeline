-- mart_weekly_revenue.sql
-- Dashboard Tile 1: Weekly revenue by category
-- Powers: line/bar chart showing revenue trends per week per category

with fact as (
    select * from {{ ref('fact_order_items') }}
)

select
    year_week,
    order_year,
    order_week,
    -- use Monday of that week as the date axis for the chart
    date_trunc(order_date, week(monday))        as week_start_date,
    category,

    -- revenue metrics
    round(sum(line_total), 2)                   as total_revenue,
    round(sum(line_cost), 2)                    as total_cost,
    round(sum(gross_profit), 2)                 as total_gross_profit,
    round(
        sum(gross_profit) / nullif(sum(line_total), 0) * 100,
        2
    )                                           as gross_margin_pct,

    -- volume metrics
    sum(quantity)                               as units_sold,
    count(distinct order_id)                    as total_orders,
    count(distinct line_item_id)                as total_line_items,

    -- average metrics
    round(sum(line_total) / nullif(count(distinct order_id), 0), 2) as avg_order_value,
    round(sum(line_total) / nullif(sum(quantity), 0), 2)            as avg_unit_revenue

from fact
group by
    year_week,
    order_year,
    order_week,
    week_start_date,
    category
order by
    week_start_date,
    category
