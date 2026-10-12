with p as (
    select * from {{ ref('stg_cot__positions') }}
),

calc as (
    select
        row_key,
        source,
        contract_key,
        report_type,
        report_date,
        open_interest,
        managed_money_long,
        managed_money_short,
        managed_money_net,
        producer_net,
        swap_net,
        managed_money_net - lag(managed_money_net) over w      as managed_money_net_change,
        100 * managed_money_net / nullif(open_interest, 0)      as managed_money_net_pct_oi,
        avg(managed_money_net)    over w52                      as mm_net_52w_avg,
        stddev(managed_money_net) over w52                      as mm_net_52w_std
    from p
    window
        w   as (partition by contract_key, report_type order by report_date),
        w52 as (partition by contract_key, report_type order by report_date
                rows between 51 preceding and current row)
)

select
    *,
    round((managed_money_net - mm_net_52w_avg) / nullif(mm_net_52w_std, 0), 2) as crowding_zscore
from calc
