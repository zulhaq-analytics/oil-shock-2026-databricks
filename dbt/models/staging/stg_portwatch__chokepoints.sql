select
    concat_ws('|', portid, date)                                         as row_key,
    to_date(date)                                                        as transit_date,
    portid                                                               as chokepoint_id,
    portname                                                             as chokepoint_name,
    cast(get_json_object(attributes_json, '$.n_total') as int)           as ships_total,
    cast(get_json_object(attributes_json, '$.n_tanker') as int)          as ships_tanker,
    cast(get_json_object(attributes_json, '$.n_container') as int)       as ships_container,
    cast(get_json_object(attributes_json, '$.n_dry_bulk') as int)        as ships_dry_bulk,
    cast(get_json_object(attributes_json, '$.n_general_cargo') as int)   as ships_general_cargo,
    cast(get_json_object(attributes_json, '$.n_roro') as int)            as ships_roro,
    cast(get_json_object(attributes_json, '$.capacity') as double)       as capacity_total_mt,
    cast(get_json_object(attributes_json, '$.capacity_tanker') as double) as capacity_tanker_mt,
    loaded_at
from {{ source('bronze', 'portwatch_chokepoints') }}
