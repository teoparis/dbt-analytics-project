{{
    config(
        materialized='table',
        tags=['mart', 'core', 'customers']
    )
}}

/*
  Customer dimension with lifetime value metrics and activity segmentation.

  This is a Type-1 SCD (overwrite) — the most recent state of each customer
  is preserved. If full history is needed, create a snapshot on stg_customers
  and join here by customer_id + dbt_valid_to.

  Segmentation logic:
    new        — first order within the last 30 days
    active     — last order within 90 days, not new
    at_risk    — last order 90-180 days ago
    churned    — no order in the last 180 days (or no orders at all)
*/

with customers as (

    select * from {{ ref('stg_customers') }}

),

lifetime as (

    select * from {{ ref('int_customer_lifetime') }}

),

country_ref as (

    select
        country_code,
        country_name,
        region

    from {{ ref('country_codes') }}

),

joined as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }}   as customer_sk,

        -- natural key
        c.customer_id,

        -- contact info (PII — ensure downstream access controls are applied)
        c.email,
        c.first_name,
        c.last_name,

        -- geography
        c.country_code,
        cr.country_name,
        cr.region,
        c.timezone,

        -- account attributes
        c.acquisition_channel,
        c.referral_source,
        c.account_status,
        c.is_active,
        c.is_email_verified,
        c.marketing_opt_in,

        -- lifetime metrics
        coalesce(lt.total_orders, 0)            as total_orders,
        coalesce(lt.total_revenue_cents, 0)     as total_revenue_cents,
        coalesce(lt.total_revenue_eur, 0)       as total_revenue_eur,
        coalesce(lt.avg_order_value_cents, 0)   as avg_order_value_cents,
        coalesce(lt.avg_order_value_eur, 0)     as avg_order_value_eur,
        lt.first_order_date,
        lt.last_order_date,
        lt.days_since_last_order,
        lt.customer_tenure_days,
        lt.orders_per_30_days,
        lt.last_order_country,
        lt.recency_tier,

        -- activity segment (used by marketing for targeting)
        case
            when lt.total_orders is null
                then 'no_orders'
            when lt.first_order_date >= dateadd('day', -30, current_date())
                then 'new'
            when lt.days_since_last_order <= 90
                then 'active'
            when lt.days_since_last_order <= 180
                then 'at_risk'
            else
                'churned'
        end                                     as customer_segment,

        -- value tier (based on total revenue — thresholds are business-defined)
        case
            when coalesce(lt.total_revenue_cents, 0) = 0        then 'no_revenue'
            when lt.total_revenue_cents < 10000                  then 'low'      -- < €100
            when lt.total_revenue_cents < 100000                 then 'mid'      -- < €1,000
            when lt.total_revenue_cents < 1000000                then 'high'     -- < €10,000
            else                                                      'vip'      -- >= €10,000
        end                                     as revenue_tier,

        -- is the customer acquired through a paid channel?
        acquisition_channel in (
            'paid_search', 'paid_social', 'display', 'affiliate'
        )                                       as is_paid_acquisition,

        -- timestamps
        c.created_at,
        c.updated_at,
        c.last_login_at,

        -- audit
        current_timestamp()                     as _dbt_updated_at

    from customers c
    left join lifetime lt
        on c.customer_id = lt.customer_id
    left join country_ref cr
        on c.country_code = cr.country_code

)

select * from joined
