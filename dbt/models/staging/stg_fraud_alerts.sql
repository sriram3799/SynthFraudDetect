{{
  config(
    materialized = 'table',
    description  = 'Fraud-only transaction events, exploded to one row per fraud flag. '
                   'Materialised as a table so the UNNEST cost is paid once per dbt run.'
  )
}}

/*
  Source: sentinel_raw.fraud_alerts (Iceberg, DAY-partitioned on event_time)
  Each source row may carry multiple fraud flags (e.g. a transaction that
  triggers both VELOCITY_BREACH and LOCATION_JUMP in the same window).
  Unnesting to one-row-per-flag makes aggregations in mart models trivial.
*/

with source as (

    select *
    from {{ source('sentinel_raw', 'fraud_alerts') }}
    where transaction_id is not null
      and is_fraud = true

),

unnested as (

    select
        -- Identity
        transaction_id,
        user_id,
        merchant_id,
        cast(sequence_number as bigint)         as sequence_number,
        cast(generator_seed  as bigint)         as generator_seed,

        -- Transaction payload
        cast(amount_usd as double)              as amount_usd,
        upper(trim(currency))                   as currency,

        -- Timestamps
        to_timestamp(cast(event_time as bigint) / 1000.0)
                                                as event_time,
        to_timestamp(cast(flink_processing_timestamp as bigint) / 1000.0)
                                                as flink_processing_timestamp,
        cast(processing_time_ms as bigint)      as processing_time_ms,

        -- Network and device
        ip_address,
        cast(latitude  as double)               as latitude,
        cast(longitude as double)               as longitude,
        device_fingerprint,
        session_id,

        -- Fraud enrichment
        cast(risk_score as double)              as risk_score,
        fraud_flags,
        cast(is_fraud as boolean)               as is_fraud,

        -- Exploded: one row per flag
        unnest(fraud_flags)                     as fraud_flag

    from source

)

select * from unnested
