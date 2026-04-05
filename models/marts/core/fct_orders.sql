{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        cluster_by=['order_month', 'country_code'],
        tags=['mart', 'core', 'orders']
    )
}}

/*
  Central orders fact table. Incremental merge on order_id + updated_at.

  Snowflake clustering: (order_month, country_code) gives the warehouse
  a good starting partition for time-range and geo filters, which are
  the most common query patterns in BI dashboards.
*/

with enriched_orders as (

    select * from {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
    -- Only process orders modified since the last run.
    -- We add a small buffer to handle late-arriving events.
    where updated_at > (
        select dateadd('hour', -3, max(updated_at))
        from {{ this }}
    )
    {% endif %}

),

products_per_order as (

    -- Derive top product category per order (useful for revenue attribution)
    select
        oi.order_id,
        p.category                              as top_category,
        sum(oi.line_total_cents)                as category_revenue_cents

    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_products') }} p
        on oi.product_id = p.product_id
    group by 1, 2
    qualify row_number() over (
        partition by oi.order_id
        order by sum(oi.line_total_cents) desc
    ) = 1

),

final as (

    select
        -- surrogate key (deterministic, environment-safe)
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}  as order_sk,

        -- natural keys
        o.order_id,
        o.customer_id,

        -- order descriptors
        o.status,
        o.payment_method,
        o.country_code,
        o.currency,
        p.top_category                                          as primary_product_category,

        -- boolean flags
        o.is_paid,
        o.is_cancelled,
        o.is_refunded,
        o.has_discount,
        o.customer_is_active,

        -- item metrics
        o.item_count,
        o.total_units,

        -- financial amounts (cents — raw storage)
        o.subtotal_cents,
        o.order_level_discount_cents,
        o.items_discount_cents,
        o.tax_cents,
        o.shipping_cents,
        o.total_cents,
        o.items_gross_cents,
        o.items_net_cents,
        o.avg_unit_value_cents,

        -- EUR equivalents (for reporting convenience)
        {{ cents_to_euros('o.total_cents') }}                   as total_eur,
        {{ cents_to_euros('o.items_net_cents') }}               as items_net_eur,
        {{ cents_to_euros('o.tax_cents') }}                     as tax_eur,
        {{ cents_to_euros('o.shipping_cents') }}                as shipping_eur,

        -- acquisition context
        o.acquisition_channel,

        -- date dimensions
        o.order_date,
        o.order_day,
        o.order_week,
        o.order_month,
        o.shipped_at,
        o.delivered_at,
        o.updated_at,
        o.days_to_ship,

        -- audit columns
        current_timestamp()                                     as _dbt_updated_at

    from enriched_orders o
    left join products_per_order p
        on o.order_id = p.order_id

)

select * from final
