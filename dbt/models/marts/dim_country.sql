select
    iso2                                             as country_code,
    country_name,
    region,
    opec_member,
    opec_plus_partner,
    opec_member or opec_plus_partner                 as opec_plus,
    hormuz_exporter
from {{ ref('ref_countries') }}
