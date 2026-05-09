{{
  config(
    materialized = 'table',
    description  = 'Users who triggered VELOCITY_BREACH in the last 24 hours, '
                   'ranked by breach count. Primary watchlist for analyst review.'
  )
}}

/*
  Business question: Which user accounts are currently showing velocity-based
  fraud behaviour and should be reviewed or blocked?

  24-hour window keeps the watchlist actionable — accounts with older breach
  events fall off automatically. Analysts can use forensics/user_fraud_history.sql
  for historical deep-dives on specific users.
*/

with recent_breaches as (

    select
        user_id,
        transaction_id,
        amount_usd,
        risk_score,
        ip_address,
        device_fingerprint,
        event_time
    from {{ ref('stg_fraud_alerts') }}
    where fraud_flag = 'VELOCITY_BREACH'
      and event_time >= current_timestamp - interval '24 hours'

),

aggregated as (

    select
        user_id,
        count(distinct transaction_id)          as breach_event_count,
        round(sum(amount_usd), 2)               as total_flagged_amount_usd,
        round(max(risk_score), 3)               as max_risk_score,
        count(distinct ip_address)              as distinct_ips,
        count(distinct device_fingerprint)      as distinct_devices,
        min(event_time)                         as first_breach,
        max(event_time)                         as last_breach
    from recent_breaches
    group by user_id

)

select *
from aggregated
where breach_event_count > {{ var("velocity_threshold") }}
order by breach_event_count desc, max_risk_score desc
