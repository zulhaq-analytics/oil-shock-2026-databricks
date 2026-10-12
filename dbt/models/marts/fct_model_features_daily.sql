with spine as (
    select date_day, day_of_week, is_driving_season, is_heating_season,
           is_hurricane_season, is_war_period
    from {{ ref('dim_date') }}
    where date_day between '2015-01-01' and current_date()
),

px as (
    select price_date, brent, wti, brent_wti_spread, gasoline_crack, diesel_crack, jet_crack
    from {{ ref('fct_spread_daily') }}
),

hh as (
    select price_date, price as henry_hub
    from {{ ref('fct_price_daily') }}
    where series_id = 'RNGWHHD'
),

macro as (
    select date_add(obs_date, 1) as avail_date,
           usd_index, fed_funds_rate, treasury_10y_yield, vix_model_only
    from {{ ref('fct_macro_daily') }}
),

pos_ranked as (
    select date_add(report_date, 3) as avail_date, contract_key,
           managed_money_net_pct_oi, crowding_zscore,
           row_number() over (partition by contract_key, report_date
                              order by case when report_type = 'futures_only' then 1 else 2 end) as rn
    from {{ ref('fct_positioning_weekly') }}
    where contract_key in ('brent_ice', 'wti_nymex')
),

pos as (
    select avail_date,
           max(case when contract_key = 'brent_ice' then managed_money_net_pct_oi end) as brent_mm_net_pct_oi,
           max(case when contract_key = 'brent_ice' then crowding_zscore end)          as brent_crowding_z,
           max(case when contract_key = 'wti_nymex' then managed_money_net_pct_oi end) as wti_mm_net_pct_oi,
           max(case when contract_key = 'wti_nymex' then crowding_zscore end)          as wti_crowding_z
    from pos_ranked
    where rn = 1
    group by avail_date
),

choke as (
    select date_add(transit_date, 7) as avail_date,
           max(case when lower(chokepoint_name) like '%hormuz%'    then ships_tanker_pct_of_2025 end) as hormuz_tanker_pct,
           max(case when lower(chokepoint_name) like '%hormuz%'    then ships_total_pct_of_2025 end)  as hormuz_total_pct,
           max(case when lower(chokepoint_name) like '%mandeb%'    then ships_tanker_pct_of_2025 end) as mandeb_tanker_pct,
           max(case when lower(chokepoint_name) like '%suez%'      then ships_tanker_pct_of_2025 end) as suez_tanker_pct,
           max(case when lower(chokepoint_name) like '%good hope%' then ships_tanker_pct_of_2025 end) as cape_tanker_pct
    from {{ ref('fct_chokepoint_daily') }}
    group by transit_date
),

risk as (
    select news_date as avail_date,
           max(case when event_region = 'Gulf' then oil_risk_zscore end)             as gulf_risk_surprise_z,
           max(case when event_region = 'Gulf' then oil_risk_level_index end)        as gulf_risk_level_index,
           max(case when event_region = 'Gulf' then talks_zscore end)                as gulf_talks_z,
           max(case when event_region = 'Global' then oil_risk_zscore end)           as global_risk_surprise_z,
           max(case when event_region = 'Red Sea' then oil_risk_zscore end)          as redsea_risk_surprise_z,
           max(case when event_region = 'Russia/Black Sea' then oil_risk_zscore end) as russia_risk_surprise_z,
           max(cast(is_data_gap as int)) = 1                                         as risk_data_gap
    from {{ ref('fct_risk_index_daily') }}
    group by news_date
),

us as (
    select date_add(week_ending, 5) as avail_date,
           crude_stocks_commercial_kbbl, crude_stocks_change_kbbl,
           refinery_utilization_pct, crude_production_kbd
    from {{ ref('fct_us_weekly_balance') }}
),

storm as (
    select storm_date, storms_in_gulf, hurricane_in_gulf
    from {{ ref('fct_gulf_storm_daily') }}
),

mm as (
    select date_add(add_months(month_start, 1), 14) as avail_date,
           us_inflation_yoy_pct, us_industrial_production_yoy_pct
    from {{ ref('fct_macro_monthly') }}
),

cli as (
    select date_add(add_months(period_date, 1), 12) as avail_date,
           max(case when area_code = 'CHN' then cli_value end)     as china_cli,
           max(case when area_code = 'CHN' then cli_change_3m end) as china_cli_change_3m,
           max(case when area_code = 'USA' then cli_value end)     as us_cli,
           max(case when area_code = 'USA' then cli_change_3m end) as us_cli_change_3m
    from {{ ref('fct_leading_indicator_monthly') }}
    group by period_date
),

joined as (
    select sp.*,
           px.brent, px.wti, px.brent_wti_spread, px.gasoline_crack, px.diesel_crack, px.jet_crack,
           hh.henry_hub,
           macro.usd_index, macro.fed_funds_rate, macro.treasury_10y_yield, macro.vix_model_only,
           pos.brent_mm_net_pct_oi, pos.brent_crowding_z, pos.wti_mm_net_pct_oi, pos.wti_crowding_z,
           choke.hormuz_tanker_pct, choke.hormuz_total_pct, choke.mandeb_tanker_pct,
           choke.suez_tanker_pct, choke.cape_tanker_pct,
           risk.gulf_risk_surprise_z, risk.gulf_risk_level_index, risk.gulf_talks_z,
           risk.global_risk_surprise_z, risk.redsea_risk_surprise_z, risk.russia_risk_surprise_z,
           risk.risk_data_gap,
           us.crude_stocks_commercial_kbbl, us.crude_stocks_change_kbbl,
           us.refinery_utilization_pct, us.crude_production_kbd,
           coalesce(storm.storms_in_gulf, 0)        as storms_in_gulf,
           coalesce(storm.hurricane_in_gulf, false) as hurricane_in_gulf,
           mm.us_inflation_yoy_pct, mm.us_industrial_production_yoy_pct,
           cli.china_cli, cli.china_cli_change_3m, cli.us_cli, cli.us_cli_change_3m
    from spine sp
    left join px    on px.price_date = sp.date_day
    left join hh    on hh.price_date = sp.date_day
    left join macro on macro.avail_date = sp.date_day
    left join pos   on pos.avail_date = sp.date_day
    left join choke on choke.avail_date = sp.date_day
    left join risk  on risk.avail_date = sp.date_day
    left join us    on us.avail_date = sp.date_day
    left join storm on storm.storm_date = sp.date_day
    left join mm    on mm.avail_date = sp.date_day
    left join cli   on cli.avail_date = sp.date_day
),

filled as (
    select
        date_day, day_of_week, is_driving_season, is_heating_season, is_hurricane_season, is_war_period,
        brent, wti, brent_wti_spread, gasoline_crack, diesel_crack, jet_crack, henry_hub,
        last_value(usd_index, true)                         over w as usd_index,
        last_value(fed_funds_rate, true)                    over w as fed_funds_rate,
        last_value(treasury_10y_yield, true)                over w as treasury_10y_yield,
        last_value(vix_model_only, true)                    over w as vix_model_only,
        last_value(brent_mm_net_pct_oi, true)               over w as brent_mm_net_pct_oi,
        last_value(brent_crowding_z, true)                  over w as brent_crowding_z,
        last_value(wti_mm_net_pct_oi, true)                 over w as wti_mm_net_pct_oi,
        last_value(wti_crowding_z, true)                    over w as wti_crowding_z,
        last_value(hormuz_tanker_pct, true)                 over w as hormuz_tanker_pct,
        last_value(hormuz_total_pct, true)                  over w as hormuz_total_pct,
        last_value(mandeb_tanker_pct, true)                 over w as mandeb_tanker_pct,
        last_value(suez_tanker_pct, true)                   over w as suez_tanker_pct,
        last_value(cape_tanker_pct, true)                   over w as cape_tanker_pct,
        gulf_risk_surprise_z,
        max(gulf_risk_surprise_z)   over w3 as gulf_risk_surprise_z_3d_max,
        gulf_risk_level_index,
        gulf_talks_z,
        max(gulf_talks_z)           over w3 as gulf_talks_z_3d_max,
        global_risk_surprise_z,
        max(global_risk_surprise_z) over w3 as global_risk_surprise_z_3d_max,
        max(redsea_risk_surprise_z) over w3 as redsea_risk_surprise_z_3d_max,
        max(russia_risk_surprise_z) over w3 as russia_risk_surprise_z_3d_max,
        coalesce(risk_data_gap, false) as risk_data_gap,
        last_value(crude_stocks_commercial_kbbl, true)      over w as crude_stocks_commercial_kbbl,
        last_value(crude_stocks_change_kbbl, true)          over w as crude_stocks_change_kbbl,
        last_value(refinery_utilization_pct, true)          over w as refinery_utilization_pct,
        last_value(crude_production_kbd, true)              over w as us_crude_production_kbd,
        storms_in_gulf,
        hurricane_in_gulf,
        last_value(us_inflation_yoy_pct, true)              over w as us_inflation_yoy_pct,
        last_value(us_industrial_production_yoy_pct, true)  over w as us_industrial_production_yoy_pct,
        last_value(china_cli, true)                         over w as china_cli,
        last_value(china_cli_change_3m, true)               over w as china_cli_change_3m,
        last_value(us_cli, true)                            over w as us_cli,
        last_value(us_cli_change_3m, true)                  over w as us_cli_change_3m
    from joined
    window w  as (order by date_day rows between unbounded preceding and current row),
           w3 as (order by date_day rows between 2 preceding and current row)
),

trading as (
    select *,
           ln(brent / lag(brent) over (order by date_day))    as brent_ret_1d,
           ln(brent / lag(brent, 5) over (order by date_day)) as brent_ret_5d_past
    from filled
    where brent is not null
)

select
    date_day as price_date,
    *except (date_day),
    stddev(brent_ret_1d) over (order by date_day rows between 20 preceding and 1 preceding) as brent_vol_20d
from trading
