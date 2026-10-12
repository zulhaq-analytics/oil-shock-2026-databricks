select
    j.row_key,
    j.country_code,
    j.period_date,
    j.energy_product                       as product_code,
    p.product_name,
    p.product_group,
    j.flow_breakdown                       as flow_code,
    f.flow_name,
    f.measure,
    j.unit_measure,
    j.obs_value                            as value,
    j.assessment_code
from {{ ref('stg_jodi__oil') }} j
join {{ ref('ref_jodi_products') }} p on j.energy_product = p.product_code
join {{ ref('ref_jodi_flows') }} f    on j.flow_breakdown = f.flow_code
where j.is_reported
  and ((f.measure = 'flow'  and j.unit_measure = 'KBD')
    or (f.measure = 'stock' and j.unit_measure = 'KBBL'))
