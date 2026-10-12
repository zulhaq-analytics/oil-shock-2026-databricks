with stories as (
    select news_date, event_region, category_rule as category, severity_score, is_oil_relevant
    from {{ ref('int_gdelt__stories') }}
    union all
    select news_date, 'Global', category_rule as category, severity_score, is_oil_relevant
    from {{ ref('int_gdelt__stories') }}
),

bounds as (
    select min(news_date) as d0, max(news_date) as d1
    from {{ ref('int_gdelt__stories') }}
),

dates as (
    select d.date_day as news_date
    from {{ ref('dim_date') }} d
    join bounds b on d.date_day between b.d0 and b.d1
),

spine as (
    select dt.news_date, r.event_region
    from dates dt
    cross join {{ ref('dim_event_region') }} r
),

daily as (
    select
        news_date,
        event_region,
        count(*)                                                        as stories_all,
        sum(severity_score)                                             as severity_all,
        sum(case when is_oil_relevant then 1 else 0 end)                as stories_oil,
        sum(case when is_oil_relevant then severity_score else 0 end)   as severity_oil,
        sum(case when category = 'talks_or_ceasefire' then 1 else 0 end) as talks_stories
    from stories
    group by news_date, event_region
),

volume as (
    select news_date, count(*) as gdelt_events
    from {{ ref('stg_gdelt__events') }}
    group by news_date
),

volume_median as (
    select
        dt.news_date,
        coalesce(v.gdelt_events, 0) as gdelt_events,
        percentile_approx(v.gdelt_events, 0.5)
            over (order by dt.news_date rows between 30 preceding and 1 preceding) as median_30d
    from dates dt
    left join volume v on dt.news_date = v.news_date
),

gap as (
    select
        news_date,
        gdelt_events,
        coalesce(gdelt_events = 0 or gdelt_events < 0.5 * median_30d, false) as is_data_gap
    from volume_median
),

filled as (
    select
        sp.news_date,
        sp.event_region,
        g.gdelt_events,
        g.is_data_gap,
        case when g.is_data_gap then null else coalesce(d.stories_all, 0)   end as stories_all,
        case when g.is_data_gap then null else coalesce(d.severity_all, 0)  end as severity_all,
        case when g.is_data_gap then null else coalesce(d.stories_oil, 0)   end as stories_oil,
        case when g.is_data_gap then null else coalesce(d.severity_oil, 0)  end as severity_oil,
        case when g.is_data_gap then null else coalesce(d.talks_stories, 0) end as talks_stories
    from spine sp
    join gap g on sp.news_date = g.news_date
    left join daily d
      on sp.news_date = d.news_date
     and sp.event_region = d.event_region
),

baseline_2025 as (
    select
        event_region,
        avg(severity_oil)    as oil_2025_avg,
        stddev(severity_oil) as oil_2025_std
    from filled
    where year(news_date) = 2025
      and severity_oil is not null
    group by event_region
),

scored as (
    select
        f.*,
        avg(severity_oil)     over w90 as oil_90d_avg,
        stddev(severity_oil)  over w90 as oil_90d_std,
        avg(severity_all)     over w90 as all_90d_avg,
        stddev(severity_all)  over w90 as all_90d_std,
        avg(talks_stories)    over w90 as talks_90d_avg,
        stddev(talks_stories) over w90 as talks_90d_std
    from filled f
    window w90 as (partition by event_region order by news_date
                   rows between 90 preceding and 1 preceding)
)

select
    concat_ws('|', s.event_region, cast(s.news_date as string))                    as row_key,
    s.news_date,
    s.event_region,
    s.stories_oil,
    round(s.severity_oil, 1)                                                        as severity_oil,
    round((s.severity_oil - s.oil_90d_avg) / nullif(s.oil_90d_std, 0), 2)           as oil_risk_zscore,
    round(100 * s.severity_oil / nullif(b.oil_2025_avg, 0), 1)                      as oil_risk_level_index,
    round((s.severity_oil - b.oil_2025_avg) / nullif(b.oil_2025_std, 0), 2)         as oil_risk_level_zscore,
    s.stories_all,
    round(s.severity_all, 1)                                                        as severity_all,
    round((s.severity_all - s.all_90d_avg) / nullif(s.all_90d_std, 0), 2)           as broad_risk_zscore,
    s.talks_stories,
    round((s.talks_stories - s.talks_90d_avg) / nullif(s.talks_90d_std, 0), 2)      as talks_zscore,
    s.is_data_gap,
    s.gdelt_events
from scored s
left join baseline_2025 b on s.event_region = b.event_region