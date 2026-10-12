select
    concat_ws('|', o.series_id, o.date)    as row_key,
    o.series_id,
    m.title                                as series_title,
    m.units,
    m.frequency,
    to_date(o.date)                        as obs_date,
    o.value,
    m.has_copyright_note,
    o.loaded_at
from {{ source('bronze', 'fred_observations') }} o
left join {{ source('bronze', 'fred_series_meta') }} m
    on o.series_id = m.series_id
where o.value is not null
