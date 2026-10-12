select
    globaleventid                                          as event_id,
    to_date(event_date, 'yyyyMMdd')                        as event_date,
    to_date(substr(date_added, 1, 8), 'yyyyMMdd')          as news_date,
    case when length(date_added) = 14
         then to_timestamp(date_added, 'yyyyMMddHHmmss') end as news_ts,
    datediff(to_date(substr(date_added, 1, 8), 'yyyyMMdd'),
             to_date(event_date, 'yyyyMMdd'))              as days_event_before_news,
    actor1_code, actor1_name, actor1_country,
    actor2_code, actor2_name, actor2_country,
    is_root_event,
    event_code, event_base_code, event_root_code,
    quad_class, goldstein,
    num_mentions, num_sources, num_articles, avg_tone,
    geo_type, geo_name, geo_country, geo_lat, geo_long,
    source_url,
    matched_by,
    source_file,
    loaded_at
from {{ source('bronze', 'gdelt_events') }}
