with s as (
    select news_date, event_region, severity_score, is_oil_relevant from {{ ref('int_gdelt__stories') }}
    union all
    select news_date, 'Global', severity_score, is_oil_relevant from {{ ref('int_gdelt__stories') }}
),

daily as (
    select
        news_date,
        event_region,
        count(*)                                                    as stories_all,
        sum(severity_score)                                         as severity_all,
        sum(case when is_oil_relevant then 1 else 0 end)            as stories_oil,
        sum(case when is_oil_relevant then severity_score else 0 end) as severity_oil
    from s
    group by news_date, event_region
),

volume as (
    select news_date, count(*) as gdelt_events
    from {{ ref('stg_gdelt__events') }}
    group by news_date
),

volume_flag as (
    select
        news_date,
        gdelt_events,
        gdelt_events < 0.5 * percentile_approx(gdelt_events, 0.5)
            over (order by news_date rows between 30 preceding and 1 preceding) as is_data_gap
    from volume
),

scored as (
    select
        d.*,
        avg(severity_oil)    over w90 as oil_90d_avg,
        stddev(severity_oil) over w90 as oil_90d_std,
        avg(severity_all)    over w90 as all_90d_avg,
        stddev(severity_all) over w90 as all_90d_std
    from daily d
    window w90 as (partition by event_region order by news_date
                   rows between 90 preceding and 1 preceding)
)

select
    concat_ws('|', s.event_region, cast(s.news_date as string)) as row_key,
    s.news_date,
    s.event_region,
    s.stories_oil,
    round(s.severity_oil, 1)                                                  as severity_oil,
    round((s.severity_oil - s.oil_90d_avg) / nullif(s.oil_90d_std, 0), 2)     as oil_risk_zscore,
    s.stories_all,
    round(s.severity_all, 1)                                                  as severity_all,
    round((s.severity_all - s.all_90d_avg) / nullif(s.all_90d_std, 0), 2)     as broad_risk_zscore,
    coalesce(v.is_data_gap, false)                                            as is_data_gap,
    v.gdelt_events
from scored s
left join volume_flag v on s.news_date = v.news_date
