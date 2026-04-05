{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        -- primary key
        id                                          as order_id,

        -- foreign keys
        customer_id,

        -- order metadata
        lower(trim(status))                         as status,
        lower(trim(payment_method))                 as payment_method,
        upper(trim(country_code))                   as country_code,
        lower(trim(currency))                       as currency,

        -- amounts (stored in cents to avoid floating-point drift)
        coalesce(subtotal_cents, 0)                 as subtotal_cents,
        coalesce(discount_cents, 0)                 as discount_cents,
        coalesce(tax_cents, 0)                      as tax_cents,
        coalesce(shipping_cents, 0)                 as shipping_cents,
        coalesce(total_cents, 0)                    as total_cents,

        -- derived booleans — evaluated once here, reused downstream
        status in ('shipped', 'delivered')          as is_paid,
        status = 'cancelled'                        as is_cancelled,
        status = 'refunded'                         as is_refunded,
        discount_cents > 0                          as has_discount,

        -- timestamps
        cast(created_at as timestamp)               as order_date,
        cast(shipped_at as timestamp)               as shipped_at,
        cast(delivered_at as timestamp)             as delivered_at,
        cast(updated_at as timestamp)               as updated_at,

        -- derived date dimensions (avoids repeated date_trunc in marts)
        date_trunc('day',   cast(created_at as timestamp)) as order_day,
        date_trunc('week',  cast(created_at as timestamp)) as order_week,
        date_trunc('month', cast(created_at as timestamp)) as order_month,

        -- shipping duration (null when order not yet shipped)
        case
            when shipped_at is not null
            then datediff(
                'day',
                cast(created_at as timestamp),
                cast(shipped_at  as timestamp)
            )
        end                                         as days_to_ship,

        -- ingestion metadata
        _fivetran_synced                            as _loaded_at

    from source

)

select * from renamed
