select
    concat_ws('|', storm_id, obs_time)     as row_key,
    storm_id,
    name                                   as storm_name,
    to_timestamp(obs_time)                 as obs_ts,
    year(to_timestamp(obs_time))           as season,
    record_id,
    status,
    latitude,
    longitude,
    max_wind_kt,
    min_pressure_mb,
    source_file,
    loaded_at
from {{ source('bronze', 'noaa_hurdat2') }}
