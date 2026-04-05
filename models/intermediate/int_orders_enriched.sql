{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'orders']
    )
}}

/*
  Joins orders with customers and aggregates order-item metrics per order.
  This is an ephemeral model — it compiles into a CTE in the consuming mart
  and does not create a physical table.

  Grain: one row per order.
*/

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select
        customer_id,
        email,
        first_name,
        last_name,
        country_code,
        acquisition_channel,
        is_active as customer_is_active

    from {{ ref('stg_customers') }}

),

order_items as (

    select
        order_id,
        count(distinct order_item_id)           as item_count,
        sum(quantity)                           as total_units,
        sum(line_gross_cents)                   as gross_amount_cents,
        sum(discount_cents)                     as discount_amount_cents,
        sum(line_total_cents)                   as net_amount_cents

    from {{ ref('stg_order_items') }}
    group by 1

),

joined as (

    select
        -- order keys
        o.order_id,
        o.customer_id,

        -- customer context
        c.email                                 as customer_email,
        c.first_name                            as customer_first_name,
        c.last_name                             as customer_last_name,
        c.acquisition_channel,
        c.customer_is_active,

        -- order metadata
        o.status,
        o.payment_method,
        o.country_code,
        o.currency,
        o.is_paid,
        o.is_cancelled,
        o.is_refunded,
        o.has_discount,

        -- order-level amounts (cents)
        o.subtotal_cents,
        o.discount_cents                        as order_level_discount_cents,
        o.tax_cents,
        o.shipping_cents,
        o.total_cents,

        -- item-level aggregates
        coalesce(oi.item_count, 0)              as item_count,
        coalesce(oi.total_units, 0)             as total_units,
        coalesce(oi.gross_amount_cents, 0)      as items_gross_cents,
        coalesce(oi.discount_amount_cents, 0)   as items_discount_cents,
        coalesce(oi.net_amount_cents, 0)        as items_net_cents,

        -- average unit value for the order
        case
            when coalesce(oi.total_units, 0) > 0
            then round(oi.net_amount_cents::float / oi.total_units, 2)
        end                                     as avg_unit_value_cents,

        -- timestamps
        o.order_date,
        o.order_day,
        o.order_week,
        o.order_month,
        o.shipped_at,
        o.delivered_at,
        o.updated_at,
        o.days_to_ship

    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join order_items oi
        on o.order_id = oi.order_id

)

select * from joined
