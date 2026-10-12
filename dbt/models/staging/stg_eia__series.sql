with src as (
    select * from {{ source('bronze', 'eia_series') }}
)

select
    concat_ws('|', series_id, period, coalesce(vintage, '')) as row_key,
    series_id,
    series_label,
    route,
    frequency,
    case
        when frequency = 'monthly' then to_date(concat(period, '-01'))
        else to_date(period)
    end                                             as period_date,
    period                                          as period_raw,
    value,
    units,
    nullif(vintage, '')                             as steo_vintage,
    case when route = 'steo' then 'forecast' else 'actual' end as data_kind,
    loaded_at
from src
where value is not null
