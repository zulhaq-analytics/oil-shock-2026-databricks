select
    concat_ws('|', category, ref_area, time_period, energy_product, flow_breakdown, unit_measure) as row_key,
    category,
    ref_area                                as country_code,
    to_date(concat(time_period, '-01'))     as period_date,
    energy_product,
    flow_breakdown,
    unit_measure,
    obs_value,
    obs_value is not null                   as is_reported,
    assessment_code,
    source_file,
    loaded_at
from {{ source('bronze', 'jodi_oil') }}
