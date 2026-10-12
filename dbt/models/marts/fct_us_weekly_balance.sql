with w as (
    select
        period_date as week_ending,
        max(case when series_id = 'WCESTUS1' then value end) as crude_stocks_commercial_kbbl,
        max(case when series_id = 'WCSSTUS1' then value end) as crude_stocks_spr_kbbl,
        max(case when series_id = 'WGTSTUS1' then value end) as gasoline_stocks_kbbl,
        max(case when series_id = 'WDISTUS1' then value end) as distillate_stocks_kbbl,
        max(case when series_id = 'WCRFPUS2' then value end) as crude_production_kbd,
        max(case when series_id = 'WPULEUS3' then value end) as refinery_utilization_pct,
        max(case when series_id = 'WRPUPUS2' then value end) as product_supplied_kbd,
        max(case when series_id = 'WCRIMUS2' then value end) as crude_imports_kbd,
        max(case when series_id = 'WCREXUS2' then value end) as crude_exports_kbd
    from {{ ref('stg_eia__series') }}
    where frequency = 'weekly' and data_kind = 'actual'
    group by period_date
)

select
    *,
    crude_stocks_commercial_kbbl
      - lag(crude_stocks_commercial_kbbl) over (order by week_ending) as crude_stocks_change_kbbl,
    crude_stocks_spr_kbbl
      - lag(crude_stocks_spr_kbbl) over (order by week_ending)        as spr_change_kbbl,
    crude_imports_kbd - crude_exports_kbd                              as crude_net_imports_kbd
from w
