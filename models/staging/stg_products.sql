{{
    config(
        materialized='view',
        tags=['staging', 'products']
    )
}}

with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        -- primary key
        id                                                  as product_id,

        -- identifiers
        upper(trim(sku))                                    as sku,
        trim(name)                                          as product_name,

        -- categorisation (normalised to lowercase)
        lower(trim(category))                               as category,
        lower(trim(subcategory))                            as subcategory,

        -- pricing (cents — integer arithmetic avoids rounding errors)
        coalesce(unit_price_cents, 0)                       as unit_price_cents,
        {{ cents_to_euros('unit_price_cents') }}            as unit_price_eur,

        -- product attributes
        lower(trim(billing_interval))                       as billing_interval,  -- monthly / annual / one_time
        is_active,
        is_taxable,
        coalesce(weight_grams, 0)                           as weight_grams,

        -- derived flags
        billing_interval = 'monthly'                        as is_recurring_monthly,
        billing_interval = 'annual'                         as is_recurring_annual,
        billing_interval in ('monthly', 'annual')           as is_subscription_product,

        -- timestamps
        cast(created_at as timestamp)                       as created_at,
        cast(updated_at as timestamp)                       as updated_at,

        -- ingestion metadata
        _fivetran_synced                                    as _loaded_at

    from source
    where id is not null   -- hard filter: orphan rows with no PK are useless

)

select * from renamed
