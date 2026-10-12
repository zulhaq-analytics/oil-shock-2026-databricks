{% set n = "cast(regexp_replace(get_json_object(row_json, '$.{}'), ',', '') as double)" %}

with cftc as (
    select
        'CFTC'                                         as source,
        case market_code
            when '067651' then 'wti_nymex'
            when '111659' then 'rbob_nymex'
            when '022651' then 'ulsd_nymex'
        end                                            as contract_key,
        'futures_only'                                 as report_type,
        market_name,
        to_date(report_date)                           as report_date,
        {{ n.format('open_interest_all') }}            as open_interest,
        {{ n.format('prod_merc_positions_long') }}     as producer_long,
        {{ n.format('prod_merc_positions_short') }}    as producer_short,
        {{ n.format('swap_positions_long_all') }}      as swap_long,
        {{ n.format('swap__positions_short_all') }}    as swap_short,
        {{ n.format('m_money_positions_long_all') }}   as managed_money_long,
        {{ n.format('m_money_positions_short_all') }}  as managed_money_short,
        {{ n.format('other_rept_positions_long') }}    as other_reportable_long,
        {{ n.format('other_rept_positions_short') }}   as other_reportable_short,
        {{ n.format('nonrept_positions_long_all') }}   as nonreportable_long,
        {{ n.format('nonrept_positions_short_all') }}  as nonreportable_short,
        loaded_at
    from {{ source('bronze', 'cftc_cot') }}
),

ice as (
    select
        'ICE'                                          as source,
        case
            when market_name like 'ICE Brent Crude Futures and Options%' then 'brent_ice'
            when market_name like 'ICE Brent Crude Futures%'             then 'brent_ice'
            when market_name like 'ICE Gasoil Futures and Options%'      then 'gasoil_ice'
            when market_name like 'ICE Gasoil Futures%'                  then 'gasoil_ice'
            when market_name like 'ICE Dubai%'                           then 'dubai_ice'
        end                                            as contract_key,
        case when market_name like '%and Options%' then 'futures_and_options'
             else 'futures_only' end                   as report_type,
        market_name,
        to_date(report_date)                           as report_date,
        {{ n.format('Open_Interest_All') }}            as open_interest,
        {{ n.format('Prod_Merc_Positions_Long_All') }} as producer_long,
        {{ n.format('Prod_Merc_Positions_Short_All') }} as producer_short,
        {{ n.format('Swap_Positions_Long_All') }}      as swap_long,
        {{ n.format('Swap_Positions_Short_All') }}     as swap_short,
        {{ n.format('M_Money_Positions_Long_All') }}   as managed_money_long,
        {{ n.format('M_Money_Positions_Short_All') }}  as managed_money_short,
        {{ n.format('Other_Rept_Positions_Long_All') }} as other_reportable_long,
        {{ n.format('Other_Rept_Positions_Short_All') }} as other_reportable_short,
        {{ n.format('NonRept_Positions_Long_All') }}   as nonreportable_long,
        {{ n.format('NonRept_Positions_Short_All') }}  as nonreportable_short,
        loaded_at
    from {{ source('bronze', 'ice_cot') }}
),

unioned as (
    select * from cftc
    union all
    select * from ice
)

select
    concat_ws('|', contract_key, report_type, cast(report_date as string)) as row_key,
    *,
    managed_money_long - managed_money_short       as managed_money_net,
    producer_long - producer_short                 as producer_net,
    swap_long - swap_short                         as swap_net
from unioned
