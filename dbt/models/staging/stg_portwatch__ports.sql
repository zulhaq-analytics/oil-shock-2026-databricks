select
    concat_ws('|', portid, date)                                               as row_key,
    to_date(date)                                                              as port_date,
    portid                                                                     as port_id,
    portname                                                                   as port_name,
    iso3,
    cast(get_json_object(attributes_json, '$.portcalls_tanker') as double)     as tanker_calls,
    cast(get_json_object(attributes_json, '$.export_tanker') as double)        as export_tanker_t,
    cast(get_json_object(attributes_json, '$.import_tanker') as double)        as import_tanker_t,
    cast(get_json_object(attributes_json, '$.portcalls') as double)            as port_calls_all,
    loaded_at
from {{ source('bronze', 'portwatch_ports') }}
