{{
  config(
    materialized = 'table',
    description  = 'Daily fraud volume and fraud rate. '
                   'Covers the last {{ var("lookback_days") }} days to leverage Iceberg partition pruning.'
  )
}}

/*
  Business question: How many fraudulent transactions occurred each day,
  and what percentage of total volume did they represent?

  Partition pruning: the WHERE clause on event_time pushes predicate to
  Iceberg DAY partitions so DuckDB does not scan historical Parquet files.
  All mart queries must include this filter — see forensics/README.md.
*/

with transactions as (

    select
        date_trunc('day', event_time)   as day,
        is_fraud
    from {{ ref('stg_transactions') }}
    where event_time >= current_date - interval '{{ var("lookback_days") }} days'

),

aggregated as (

    select
        day,
        count(*)                                             as total_transactions,
        sum(case when is_fraud then 1 else 0 end)            as fraud_count,
        count(*) - sum(case when is_fraud then 1 else 0 end) as legitimate_count
    from transactions
    group by day

)

select
    day,
    total_transactions,
    fraud_count,
    legitimate_count,
    round(
        cast(fraud_count as double) / nullif(total_transactions, 0) * 100,
        4
    )                                                        as fraud_rate_pct
from aggregated
order by day desc
