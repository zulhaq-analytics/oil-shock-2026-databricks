select
    snapshot_at,
    storm_id,
    name                                   as storm_name,
    classification,
    intensity_kt,
    pressure_mb,
    latitude,
    longitude,
    last_update
from {{ source('bronze', 'noaa_active_storms') }}
