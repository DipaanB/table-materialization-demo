with usage as (
  select * from WAREHOUSE_METERING_HISTORY
),

usage_date_range as (
  select
    min(start_time)        as min_date,
    max(start_time)        as max_date
  from
    usage
),

filler_daily_usage as (
  select
    0 as credits_used,
    dateadd(
      day, -seq4(), current_timestamp
    ) as start_time
  from
    table(generator(rowcount => 1000))
  where
    start_time >= (select min_date from usage_date_range) and
    start_time <= (select max_date from usage_date_range)
  order by 1
),

combined_usage as (
  select usage.credits_used, usage.start_time from usage
  union all
  select fdu.credits_used, fdu.start_time from filler_daily_usage fdu
),

daily_usage as (
    select
      to_number(sum(credits_used), 20, 1) as credits_used,
      date_trunc('day', start_time)::date as calculated_on
    from
      combined_usage
    group by
      calculated_on
    order by
      calculated_on desc
)

select * from daily_usage
