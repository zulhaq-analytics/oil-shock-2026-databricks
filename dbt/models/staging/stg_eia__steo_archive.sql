select
    release_month,
    series_id,
    to_date(concat(target_month, '-01')) as target_month,
    value,
    source_file
from {{ source('bronze_archive', 'steo_archive') }}
where value is not null
