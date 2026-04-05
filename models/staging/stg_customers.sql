{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        -- primary key
        id                                              as customer_id,

        -- identifiers
        lower(trim(email))                              as email,

        -- personal data — trim and normalise casing
        initcap(trim(first_name))                       as first_name,
        initcap(trim(last_name))                        as last_name,

        -- geography — uppercase for FK to country_codes seed
        upper(trim(country_code))                       as country_code,
        lower(trim(timezone))                           as timezone,

        -- acquisition
        lower(trim(acquisition_channel))               as acquisition_channel,
        lower(trim(referral_source))                    as referral_source,

        -- account status
        lower(trim(account_status))                     as account_status,
        account_status = 'active'                       as is_active,
        is_email_verified,

        -- marketing consent (gdpr)
        coalesce(marketing_opt_in, false)               as marketing_opt_in,

        -- timestamps
        cast(created_at    as timestamp)                as created_at,
        cast(updated_at    as timestamp)                as updated_at,
        cast(last_login_at as timestamp)                as last_login_at,

        -- ingestion metadata
        _fivetran_synced                                as _loaded_at

    from source

),

-- Deduplicate: keep the most recently updated record per customer.
-- The raw layer may contain duplicates if CDC events arrive out of order.
deduped as (

    select *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as _row_num

    from renamed

)

select
    customer_id,
    email,
    first_name,
    last_name,
    country_code,
    timezone,
    acquisition_channel,
    referral_source,
    account_status,
    is_active,
    is_email_verified,
    marketing_opt_in,
    created_at,
    updated_at,
    last_login_at,
    _loaded_at

from deduped
where _row_num = 1
