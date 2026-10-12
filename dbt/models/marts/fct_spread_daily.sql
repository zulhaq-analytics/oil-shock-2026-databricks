with wide as (
    select
        price_date,
        max(case when series_id = 'RBRTE' then price end)                      as brent,
        max(case when series_id = 'RWTC' then price end)                       as wti,
        max(case when series_id = 'EER_EPMRU_PF4_Y35NY_DPG' then price end)    as gasoline,
        max(case when series_id = 'EER_EPD2DXL0_PF4_Y35NY_DPG' then price end) as diesel,
        max(case when series_id = 'EER_EPJK_PF4_RGC_DPG' then price end)       as jet
    from {{ ref('fct_price_daily') }}
    group by price_date
)

select
    price_date,
    brent,
    wti,
    brent - wti                 as brent_wti_spread,
    gasoline - brent            as gasoline_crack,
    diesel - brent              as diesel_crack,
    jet - brent                 as jet_crack
from wide
where brent is not null
