with history as (
    select storm_id, storm_name, obs_ts, status, latitude, longitude, max_wind_kt,
           'hurdat2' as source
    from {{ ref('stg_noaa__storm_tracks') }}
),

season as (
    select storm_id, storm_name, obs_ts, status, latitude, longitude, max_wind_kt,
           'bdeck' as source
    from {{ ref('stg_noaa__bdeck') }}
    where season > (select max(season) from {{ ref('stg_noaa__storm_tracks') }})
),

live as (
    select upper(storm_id) as storm_id, upper(storm_name) as storm_name, snapshot_at as obs_ts,
           classification as status, latitude, longitude, intensity_kt as max_wind_kt,
           'active_snapshot' as source
    from {{ ref('stg_noaa__active_storms') }}
    where snapshot_at > (select coalesce(max(obs_ts), timestamp '1900-01-01') from season)
)

select
    *,
    latitude between 18 and 31 and longitude between -98 and -80 as in_gulf_of_mexico
from (
    select * from history
    union all select * from season
    union all select * from live
)
