select
    e.event_id,
    e.event_date,
    e.news_date,
    e.news_ts,
    e.days_event_before_news,
    e.actor1_name,
    e.actor2_name,
    e.event_code,
    e.event_base_code,
    e.event_root_code,
    e.goldstein,
    e.num_mentions,
    e.num_sources,
    e.avg_tone,
    e.geo_country,
    e.geo_name,
    coalesce(r.event_region, 'Other') as event_region,
    e.matched_by,
    e.source_url
from {{ ref('stg_gdelt__events') }} e
left join {{ ref('ref_gdelt_regions') }} r
    on e.geo_country = r.fips_code
where e.days_event_before_news between 0 and 7
