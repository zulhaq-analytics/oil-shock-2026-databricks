with d as (
    select
        obs_date,
        max(case when series_id = 'DTWEXBGS' then value end) as usd_index,
        max(case when series_id = 'DFF'      then value end) as fed_funds_rate,
        max(case when series_id = 'DGS10'    then value end) as treasury_10y_yield,
        max(case when series_id = 'VIXCLS'   then value end) as vix_model_only
    from {{ ref('stg_fred__observations') }}
    where series_id in ('DTWEXBGS', 'DFF', 'DGS10', 'VIXCLS')
      and obs_date >= '2000-01-01'
    group by obs_date
)

select
    *,
    case
        when vix_model_only is null then null
        when vix_model_only < 15 then 'low'
        when vix_model_only <= 25 then 'normal'
        else 'high'
    end as fear_level
from d
