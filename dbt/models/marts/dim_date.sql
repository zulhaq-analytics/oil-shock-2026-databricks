with days as (
    select explode(sequence(to_date('2002-01-01'), to_date('2027-12-31'), interval 1 day)) as date_day
)

select
    date_day,
    year(date_day)                                   as year,
    quarter(date_day)                                as quarter,
    month(date_day)                                  as month,
    date_format(date_day, 'MMM')                     as month_name,
    date_trunc('MONTH', date_day)                    as month_start,
    date_trunc('WEEK', date_day)                     as week_start,
    dayofweek(date_day)                              as day_of_week,
    dayofweek(date_day) in (1, 7)                    as is_weekend,
    month(date_day) between 6 and 8                  as is_driving_season,
    month(date_day) in (10, 11, 12, 1, 2, 3)         as is_heating_season,
    month(date_day) between 6 and 11                 as is_hurricane_season,
    date_day >= to_date('2026-02-28')                as is_war_period
from days
