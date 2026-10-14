-- Singapore MPA monthly data (data.gov.sg), one row per dataset, month, category and measure
select
    concat_ws('|', dataset, month, category, measure)  as row_key,
    dataset,
    to_date(concat(month, '-01'))                       as month_start,
    category,
    measure,
    value,
    loaded_at
from {{ source('bronze', 'sg_mpa_monthly') }}
where value is not null
  and month rlike '^[0-9]{4}-[0-9]{2}$'
