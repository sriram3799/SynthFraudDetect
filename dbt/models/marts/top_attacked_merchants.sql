{{
  config(
    materialized = 'table',
    description  = 'Top 100 merchants by fraud hit count over the last {{ var("lookback_days") }} days. '
                   'Covers both fraud pattern types. Used by analysts to identify targeted merchants.'
  )
}}

/*
  Business question: Which merchants are being hit hardest by fraud, and
  what is the total dollar value at risk?

  Source: stg_fraud_alerts (already filtered to is_fraud=true) rather than
  stg_transactions to avoid a full scan of the legitimate transaction volume.
*/

with fraud as (

    select
        merchant_id,
        transaction_id,
        amount_usd,
        fraud_flag,
        event_time
    from {{ ref('stg_fraud_alerts') }}
    where event_time >= current_date - interval '{{ var("lookback_days") }} days'

),

aggregated as (

    select
        merchant_id,
        count(distinct transaction_id)                  as fraud_hits,
        round(sum(amount_usd), 2)                       as total_fraud_amount_usd,
        round(avg(amount_usd), 2)                       as avg_fraud_amount_usd,
        count(case when fraud_flag = 'VELOCITY_BREACH'
                   then 1 end)                          as velocity_breach_count,
        count(case when fraud_flag = 'LOCATION_JUMP'
                   then 1 end)                          as location_jump_count,
        min(event_time)                                 as first_fraud_seen,
        max(event_time)                                 as last_fraud_seen
    from fraud
    group by merchant_id

)

select *
from aggregated
order by fraud_hits desc
limit 100
