with cats as (
    select explode(array('vessel_attack_or_seizure', 'infrastructure_strike', 'chokepoint',
                         'sanctions', 'opec_plus', 'talks_or_ceasefire')) as category
),

stories as (
    select news_date, event_region, category_rule as category, severity_score
    from {{ ref('int_gdelt__stories') }}
    union all
    select news_date, 'Global' as event_region, category_rule as category, severity_score
    from {{ ref('int_gdelt__stories') }}
),

daily as (
    select news_date, event_region, category,
           count(*)            as stories,
           sum(severity_score) as severity
    from stories
    where category in (select category from cats)
    group by news_date, event_region, category
),

days as (
    select news_date, is_data_gap
    from {{ ref('fct_risk_index_daily') }}
    where event_region = 'Global'
),

spine as (
    select d.news_date, d.is_data_gap, r.event_region, c.category
    from days d
    cross join {{ ref('dim_event_region') }} r
    cross join cats c
),

filled as (
    select sp.news_date, sp.event_region, sp.category, sp.is_data_gap,
           case when sp.is_data_gap then null else coalesce(dl.stories, 0) end  as stories,
           case when sp.is_data_gap then null else coalesce(dl.severity, 0) end as severity
    from spine sp
    left join daily dl
      on dl.news_date = sp.news_date
     and dl.event_region = sp.event_region
     and dl.category = sp.category
),

scored as (
    select f.*,
           avg(ln(1 + stories))    over w as log_avg_90d,
           stddev(ln(1 + stories)) over w as log_std_90d
    from filled f
    window w as (partition by event_region, category order by news_date
                 rows between 90 preceding and 1 preceding)
)

select
    concat_ws('|', event_region, category, cast(news_date as string))                as row_key,
    news_date,
    event_region,
    category,
    stories,
    round(severity, 1)                                                                as severity,
    round((ln(1 + stories) - log_avg_90d) / greatest(log_std_90d, 0.3), 2)            as stories_zscore,
    is_data_gap
from scored
