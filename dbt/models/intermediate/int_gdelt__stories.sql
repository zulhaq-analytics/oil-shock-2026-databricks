{{ config(
    materialized = 'incremental',
    unique_key = 'story_id',
    incremental_strategy = 'merge'
) }}

with recent_urls as (
    select distinct source_url
    from {{ ref('int_gdelt__events_clean') }}
    {% if is_incremental() %}
    where news_date >= (select date_sub(max(news_date), 3) from {{ this }})
    {% endif %}
),

ev as (
    select e.*
    from {{ ref('int_gdelt__events_clean') }} e
    join recent_urls u on e.source_url = u.source_url
),

ranked as (
    select
        *,
        row_number() over (partition by source_url order by num_mentions desc, event_id) as rn
    from ev
),

agg as (
    select
        source_url,
        min(news_date)                          as news_date,
        min(news_ts)                            as first_seen_ts,
        count(*)                                as num_events,
        max(num_mentions)                       as top_event_mentions,
        sum(num_mentions)                       as total_mentions,
        max(num_sources)                        as max_sources,
        min(goldstein)                          as min_goldstein,
        avg(avg_tone)                           as avg_tone,
        max(case when matched_by = 'oil_word' then 1 else 0 end) = 1 as has_oil_word
    from ev
    group by source_url
),

top_event as (
    select source_url, event_region, event_root_code, event_base_code, event_code,
           actor1_name, actor2_name, geo_name
    from ranked
    where rn = 1
),

joined as (
    select
        md5(a.source_url) as story_id,
        a.*,
        t.event_region,
        t.event_root_code,
        t.event_base_code,
        t.event_code,
        t.actor1_name,
        t.actor2_name,
        t.geo_name,
        lower(regexp_replace(
            regexp_replace(regexp_extract(a.source_url, '([^/]+)/?$', 1), '\\.(html?|php|aspx?)$', ''),
            '[-_]+', ' ')) as headline_slug,
        lower(a.source_url) as url_lc
    from agg a
    join top_event t on a.source_url = t.source_url
)

select
    story_id,
    source_url,
    headline_slug,
    news_date,
    first_seen_ts,
    event_region,
    event_root_code,
    event_base_code,
    event_code,
    actor1_name,
    actor2_name,
    geo_name,
    num_events,
    top_event_mentions,
    total_mentions,
    max_sources,
    min_goldstein,
    avg_tone,
    has_oil_word,
    case
        when url_lc rlike 'tanker|vessel|ship|cargo' and event_root_code in ('17', '18', '19', '20')
            then 'vessel_attack_or_seizure'
        when url_lc rlike 'refiner|pipeline|terminal|oilfield|oil-field|facility|depot|plant'
             and event_root_code in ('18', '19', '20')
            then 'infrastructure_strike'
        when url_lc rlike 'hormuz|strait|red-sea|bab-el|suez|chokepoint|blockade'
            then 'chokepoint'
        when event_base_code = '163' or url_lc rlike 'sanction|embargo'
            then 'sanctions'
        when url_lc rlike 'opec'
            then 'opec_plus'
        when url_lc rlike 'ceasefire|truce|talks|negotiat|peace-deal|agreement'
            then 'talks_or_ceasefire'
        when event_root_code in ('03', '04', '05', '06', '08')
            then 'diplomacy_general'
        else 'other'
    end as category_rule,
    round(
        case event_root_code
            when '20' then 5 when '19' then 4 when '18' then 4 when '17' then 3
            when '15' then 2 when '16' then 2 when '13' then 1.5 when '14' then 1
            else 0.5
        end
        * ln(1 + total_mentions)
        * case when has_oil_word then 1.5 else 1 end
    , 2) as severity_score,
    has_oil_word
      or category_rule in ('vessel_attack_or_seizure', 'infrastructure_strike', 'chokepoint', 'sanctions', 'opec_plus')
      or url_lc rlike 'tanker|vessel|refiner|pipeline|hormuz|red-sea|bab-el|suez|sanction|embargo|opec'
      as is_oil_relevant
from joined
