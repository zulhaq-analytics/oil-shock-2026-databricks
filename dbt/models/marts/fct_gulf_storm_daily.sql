select
    cast(obs_ts as date)              as storm_date,
    count(distinct storm_id)          as storms_in_gulf,
    max(max_wind_kt)                  as max_wind_kt_in_gulf,
    max(case when status = 'HU' then 1 else 0 end) = 1 as hurricane_in_gulf
from {{ ref('fct_storm_track') }}
where in_gulf_of_mexico
group by cast(obs_ts as date)
