select
    concat_ws('|', f.series_id, cast(f.obs_date as string)) as row_key,
    f.obs_date                                              as month_start,
    f.series_id,
    p.price_name,
    f.value                                                 as price,
    'USD/bbl'                                               as price_unit,
    'Source: International Monetary Fund, via FRED'         as attribution
from {{ ref('stg_fred__observations') }} f
join {{ ref('ref_price_series') }} p
    on f.series_id = p.series_id
