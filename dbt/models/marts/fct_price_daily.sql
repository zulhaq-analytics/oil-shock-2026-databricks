select
    concat_ws('|', e.series_id, cast(e.period_date as string)) as row_key,
    e.period_date                                           as price_date,
    e.series_id,
    p.price_name,
    p.price_group,
    e.value                                                 as price_original,
    p.unit_original,
    case when p.to_usd_per_bbl is null then e.value
         else e.value * p.to_usd_per_bbl end                as price,
    case when p.to_usd_per_bbl is null then p.unit_original
         else 'USD/bbl' end                                 as price_unit
from {{ ref('stg_eia__series') }} e
join {{ ref('ref_price_series') }} p
    on e.series_id = p.series_id
where e.frequency = 'daily'
  and e.data_kind = 'actual'
