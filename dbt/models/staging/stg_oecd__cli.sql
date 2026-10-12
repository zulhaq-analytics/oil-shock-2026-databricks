select
    concat_ws('|', ref_area, time_period)  as row_key,
    ref_area                               as area_code,
    area_name,
    to_date(concat(time_period, '-01'))    as period_date,
    obs_value                              as cli_value,
    loaded_at
from {{ source('bronze', 'oecd_cli') }}
where obs_value is not null
