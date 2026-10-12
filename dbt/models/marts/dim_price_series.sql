select
    series_id,
    price_name,
    price_group,
    market,
    unit_original,
    case when to_usd_per_bbl is null then unit_original else 'USD/bbl' end as unit_reported,
    to_usd_per_bbl
from {{ ref('ref_price_series') }}
