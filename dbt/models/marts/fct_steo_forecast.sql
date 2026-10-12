select
    row_key,
    steo_vintage                                            as release_month,
    series_id,
    series_label,
    units,
    period_date                                             as target_month,
    value,
    period_date >= to_date(concat(steo_vintage, '-01'))     as is_forecast,
    months_between(period_date, to_date(concat(steo_vintage, '-01'))) as months_ahead
from {{ ref('stg_eia__series') }}
where data_kind = 'forecast'
