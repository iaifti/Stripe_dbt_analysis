with dates as (
    select
        dateadd(day, seq4(), '2022-01-01'::date) as date_day
    from table(generator(rowcount => 1500))
)

select
    date_day,
    extract(day from date_day) as day,
    extract(month from date_day) as month,
    extract(year from date_day) as year,
    date_trunc('month', date_day)::date as month_start,
    date_trunc('quarter', date_day)::date as quarter_start,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (6,7) then true else false end as is_weekend
from dates
