with api as (
    select
        row_key,
        steo_vintage    as release_month,
        series_id,
        series_label,
        units,
        period_date     as target_month,
        value,
        'api'           as source
    from {{ ref('stg_eia__series') }}
    where data_kind = 'forecast'
),

labels as (
    select series_id, max(series_label) as series_label, max(units) as units
    from api
    group by series_id
),

archive as (
    select
        concat_ws('|', 'steo_archive', a.release_month, a.series_id, cast(a.target_month as string)) as row_key,
        a.release_month,
        a.series_id,
        l.series_label,
        l.units,
        a.target_month,
        a.value,
        'steo_archive'  as source
    from {{ ref('stg_eia__steo_archive') }} a
    left join labels l on l.series_id = a.series_id
    where a.release_month not in (select distinct release_month from api)
),

unioned as (
    select * from api
    union all
    select * from archive
)

select
    row_key,
    release_month,
    series_id,
    series_label,
    units,
    target_month,
    value,
    target_month >= to_date(concat(release_month, '-01'))                    as is_forecast,
    months_between(target_month, to_date(concat(release_month, '-01')))     as months_ahead,
    source
from unioned
