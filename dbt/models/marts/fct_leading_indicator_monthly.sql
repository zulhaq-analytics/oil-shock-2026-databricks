select
    row_key,
    area_code,
    area_name,
    period_date,
    cli_value,
    cli_value - lag(cli_value, 3) over (partition by area_code order by period_date) as cli_change_3m,
    case
        when cli_value - lag(cli_value, 3) over (partition by area_code order by period_date) > 0 then 'rising'
        when cli_value - lag(cli_value, 3) over (partition by area_code order by period_date) < 0 then 'falling'
        else 'flat'
    end as cli_direction
from {{ ref('stg_oecd__cli') }}
