{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

with source as (

    select * from {{ source('raw', 'order_items') }}

),

renamed as (

    select
        -- primary key
        id                                          as order_item_id,

        -- foreign keys
        order_id,
        product_id,

        -- quantities and pricing (always stored in cents)
        quantity,
        unit_price_cents,

        -- discount applied at line-item level (e.g. coupon, bulk pricing)
        coalesce(discount_cents, 0)                 as discount_cents,

        -- derived: gross revenue for this line before discounts
        quantity * unit_price_cents                 as line_gross_cents,

        -- derived: net revenue after line-level discount
        (quantity * unit_price_cents) - coalesce(discount_cents, 0)
                                                    as line_total_cents,

        -- convenience EUR columns
        {{ cents_to_euros('quantity * unit_price_cents') }}               as line_gross_eur,
        {{ cents_to_euros('(quantity * unit_price_cents) - coalesce(discount_cents, 0)') }}
                                                    as line_total_eur,

        -- ingestion metadata
        _fivetran_synced                            as _loaded_at

    from source

)

select * from renamed
