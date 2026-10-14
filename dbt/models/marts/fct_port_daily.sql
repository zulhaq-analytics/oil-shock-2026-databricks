with base as (
    select
        s.port_id, s.port_name, s.iso3, s.port_date,
        s.tanker_calls, s.export_tanker_t, s.import_tanker_t,
        avg(s.export_tanker_t) over w7 as export_tanker_t_7d_avg,
        avg(s.tanker_calls)    over w7 as tanker_calls_7d_avg
    from {{ ref('stg_portwatch__ports') }} s
    window w7 as (partition by s.port_id order by s.port_date rows between 6 preceding and current row)
),

normal_2025 as (
    select port_id, avg(export_tanker_t) as export_tanker_t_2025_avg
    from {{ ref('stg_portwatch__ports') }}
    where year(port_date) = 2025
    group by port_id
)

select
    concat_ws('|', b.port_id, cast(b.port_date as string))                          as row_key,
    b.*,
    coalesce(r.role, 'other')                                                        as port_role,
    coalesce(r.chart_label, b.port_name)                                             as chart_label,
    n.export_tanker_t_2025_avg,
    round(100 * b.export_tanker_t_7d_avg / nullif(n.export_tanker_t_2025_avg, 0), 1) as export_pct_of_2025,
    -- rough barrels: ~7.33 barrels per metric ton of crude; only meaningful for crude ports, not gas terminals
    round(b.export_tanker_t_7d_avg * 7.33 / 1000, 1)                                 as export_kbd_est
from base b
left join normal_2025 n using (port_id)
left join {{ ref('ref_gulf_ports') }} r
  on r.port_name = b.port_name and r.iso3 = b.iso3
