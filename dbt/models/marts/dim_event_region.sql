select distinct event_region
from {{ ref('ref_gdelt_regions') }}
union
select 'Other' as event_region
