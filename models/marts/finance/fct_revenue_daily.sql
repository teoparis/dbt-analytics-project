{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['revenue_date', 'country_code', 'product_category'],
        cluster_by=['revenue_date'],
        tags=['mart', 'finance', 'revenue']
    )
}}

/*
  Daily revenue aggregation for financial reporting and dashboards.

  Grain: one row per (date, country, product_category).
  Incremental: re-processes the last 3 days on each run to handle late-arriving
  order updates (e.g. refunds, delivery confirmation lag).
*/

with order_items as (

    select
        oi.order_id,
        oi.product_id,
        oi.line_gross_cents,
        oi.line_total_cents,
        oi.discount_cents

    from {{ ref('stg_order_items') }} oi

),

orders as (

    select
        o.order_id,
        o.order_day                         as revenue_date,
        o.country_code,
        o.is_paid,
        o.is_refunded,
        o.tax_cents,
        o.shipping_cents,
        o.updated_at

    from {{ ref('stg_orders') }} o

    {% if is_incremental() %}
    where o.order_day >= (
        select dateadd('day', -3, max(revenue_date))
        from {{ this }}
    )
    {% endif %}

),

products as (

    select
        product_id,
        category as product_category

    from {{ ref('stg_products') }}

),

joined as (

    select
        o.revenue_date,
        o.country_code,
        coalesce(p.product_category, 'unknown')     as product_category,

        -- order counts
        count(distinct o.order_id)                  as total_orders,
        count(distinct case when o.is_paid    then o.order_id end) as paid_orders,
        count(distinct case when o.is_refunded then o.order_id end) as refunded_orders,

        -- gross revenue (before discounts and refunds)
        sum(case when o.is_paid then oi.line_gross_cents else 0 end)    as gross_revenue_cents,

        -- discount impact
        sum(case when o.is_paid then oi.discount_cents else 0 end)      as total_discount_cents,

        -- net revenue (after discounts, before refunds)
        sum(case when o.is_paid then oi.line_total_cents else 0 end)    as net_revenue_cents,

        -- refund amount (deducted separately for P&L clarity)
        sum(case when o.is_refunded then oi.line_total_cents else 0 end) as refund_amount_cents,

        -- tax and shipping (separate lines for accounting)
        sum(case when o.is_paid then o.tax_cents else 0 end)            as tax_collected_cents,
        sum(case when o.is_paid then o.shipping_cents else 0 end)       as shipping_revenue_cents,

        -- effective revenue = net - refunds
        sum(case when o.is_paid     then oi.line_total_cents else 0 end)
        - sum(case when o.is_refunded then oi.line_total_cents else 0 end)
                                                                        as effective_revenue_cents,

        -- item metrics
        sum(case when o.is_paid then 1 else 0 end)                      as total_items_sold,
        sum(case when o.is_paid then 0 else 0 end)                      as placeholder_unit_count  -- remove if not needed

    from orders o
    inner join order_items oi
        on o.order_id = oi.order_id
    left join products p
        on oi.product_id = p.product_id
    group by 1, 2, 3

),

with_eur as (

    select
        revenue_date,
        country_code,
        product_category,
        total_orders,
        paid_orders,
        refunded_orders,
        total_items_sold,

        -- cents columns
        gross_revenue_cents,
        total_discount_cents,
        net_revenue_cents,
        refund_amount_cents,
        effective_revenue_cents,
        tax_collected_cents,
        shipping_revenue_cents,

        -- EUR equivalents for financial reports
        {{ cents_to_euros('gross_revenue_cents') }}      as gross_revenue_eur,
        {{ cents_to_euros('net_revenue_cents') }}        as net_revenue_eur,
        {{ cents_to_euros('effective_revenue_cents') }}  as effective_revenue_eur,
        {{ cents_to_euros('total_discount_cents') }}     as total_discount_eur,
        {{ cents_to_euros('refund_amount_cents') }}      as refund_amount_eur,
        {{ cents_to_euros('tax_collected_cents') }}      as tax_collected_eur,
        {{ cents_to_euros('shipping_revenue_cents') }}   as shipping_revenue_eur,

        -- derived rates
        case
            when gross_revenue_cents > 0
            then round(total_discount_cents * 100.0 / gross_revenue_cents, 2)
        end                                              as discount_rate_pct,

        case
            when paid_orders > 0
            then round(effective_revenue_cents / paid_orders, 0)
        end                                              as avg_order_value_cents,

        -- audit
        current_timestamp()                              as _dbt_updated_at

    from joined

)

select * from with_eur
