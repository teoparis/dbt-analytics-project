{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'customers']
    )
}}

/*
  Computes lifetime value metrics per customer from paid orders.
  Used by dim_customers and any downstream lifetime-value analysis.

  Grain: one row per customer_id.
*/

with orders as (

    -- Only paid, non-refunded orders contribute to revenue metrics
    select *
    from {{ ref('stg_orders') }}
    where is_paid = true
      and is_refunded = false

),

order_items as (

    select
        oi.order_id,
        sum(oi.line_total_cents) as order_net_cents

    from {{ ref('stg_order_items') }} oi
    group by 1

),

orders_with_revenue as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.country_code,
        coalesce(oi.order_net_cents, o.total_cents) as revenue_cents

    from orders o
    left join order_items oi on o.order_id = oi.order_id

),

customer_metrics as (

    select
        customer_id,

        -- volume metrics
        count(distinct order_id)                            as total_orders,
        sum(revenue_cents)                                  as total_revenue_cents,
        avg(revenue_cents)                                  as avg_order_value_cents,

        -- first & last purchase dates
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date,

        -- recency (days since last order — evaluated at query time)
        datediff(
            'day',
            max(order_date),
            current_timestamp()
        )                                                   as days_since_last_order,

        -- tenure (days between first and last order)
        datediff(
            'day',
            min(order_date),
            max(order_date)
        )                                                   as customer_tenure_days,

        -- purchase frequency (orders per month, annualised)
        case
            when datediff('day', min(order_date), max(order_date)) > 0
            then round(
                count(distinct order_id) * 30.0
                / datediff('day', min(order_date), max(order_date)),
                4
            )
        end                                                 as orders_per_30_days,

        -- geography of most recent order
        last_value(country_code) over (
            partition by customer_id
            order by order_date
            rows between unbounded preceding and unbounded following
        )                                                   as last_order_country

    from orders_with_revenue
    group by customer_id

),

enriched as (

    select
        *,
        -- convenience EUR columns
        {{ cents_to_euros('total_revenue_cents') }}     as total_revenue_eur,
        {{ cents_to_euros('avg_order_value_cents') }}   as avg_order_value_eur,

        -- simple RFM recency tier (usable without a full RFM model)
        case
            when days_since_last_order <= 30   then 'active_30d'
            when days_since_last_order <= 90   then 'active_90d'
            when days_since_last_order <= 180  then 'lapsing'
            when days_since_last_order <= 365  then 'churned_soft'
            else                                    'churned_hard'
        end                                             as recency_tier

    from customer_metrics

)

select * from enriched
