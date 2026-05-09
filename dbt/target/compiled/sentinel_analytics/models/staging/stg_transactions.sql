

/*
  Source: sentinel_raw.transactions (Iceberg, HOUR-partitioned on event_time)
  This model is the entry point for all downstream staging and mart queries.
  It is materialised as a table so the iceberg_scan cost is paid once per
  dbt run rather than on every downstream query.
*/

with source as (

    select *
    from iceberg_scan('s3://sentinel-dev/iceberg/transactions/')
    where transaction_id is not null

),

renamed as (

    select
        -- Identity
        transaction_id,
        user_id,
        merchant_id,
        cast(sequence_number as bigint)          as sequence_number,
        cast(generator_seed  as bigint)          as generator_seed,

        -- Transaction payload
        cast(amount_usd as double)               as amount_usd,
        upper(trim(currency))                    as currency,

        -- Timestamps — event_time arrives as epoch ms (bigint); cast to TIMESTAMPTZ
        to_timestamp(cast(event_time as bigint) / 1000.0)
                                                 as event_time,
        to_timestamp(cast(flink_processing_timestamp as bigint) / 1000.0)
                                                 as flink_processing_timestamp,
        cast(processing_time_ms as bigint)       as processing_time_ms,

        -- Network and device
        ip_address,
        cast(latitude  as double)                as latitude,
        cast(longitude as double)                as longitude,
        device_fingerprint,
        session_id,

        -- Fraud enrichment
        cast(risk_score as double)               as risk_score,
        fraud_flags,
        cast(is_fraud as boolean)                as is_fraud

    from source

)

select * from renamed