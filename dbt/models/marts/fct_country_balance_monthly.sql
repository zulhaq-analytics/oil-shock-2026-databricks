with b as (
    select * from {{ ref('fct_oil_balance_monthly') }}
)

select
    concat_ws('|', country_code, cast(period_date as string)) as row_key,
    country_code,
    period_date,
    max(case when product_code = 'CRUDEOIL' and flow_code = 'INDPROD'  then value end) as crude_production_kbd,
    max(case when product_code = 'NGL'      and flow_code = 'INDPROD'  then value end) as ngl_production_kbd,
    max(case when product_code = 'CRUDEOIL' and flow_code = 'TOTIMPSB' then value end) as crude_imports_kbd,
    max(case when product_code = 'CRUDEOIL' and flow_code = 'TOTEXPSB' then value end) as crude_exports_kbd,
    max(case when product_code = 'CRUDEOIL' and flow_code = 'REFINOBS' then value end) as crude_refinery_intake_kbd,
    max(case when product_code = 'TOTPRODS' and flow_code = 'TOTDEMO'  then value end) as product_demand_kbd,
    max(case when product_code = 'TOTPRODS' and flow_code = 'REFGROUT' then value end) as product_output_kbd,
    max(case when product_code = 'TOTPRODS' and flow_code = 'TOTIMPSB' then value end) as product_imports_kbd,
    max(case when product_code = 'TOTPRODS' and flow_code = 'TOTEXPSB' then value end) as product_exports_kbd,
    max(case when product_code = 'GASDIES'  and flow_code = 'TOTDEMO'  then value end) as diesel_demand_kbd,
    max(case when product_code = 'JETKERO'  and flow_code = 'TOTDEMO'  then value end) as jet_demand_kbd,
    max(case when product_code = 'GASOLINE' and flow_code = 'TOTDEMO'  then value end) as gasoline_demand_kbd,
    max(case when product_code = 'RESFUEL'  and flow_code = 'TOTDEMO'  then value end) as fuel_oil_demand_kbd,
    max(case when product_code = 'CRUDEOIL' and flow_code = 'CLOSTLV'  then value end) as crude_stocks_kbbl,
    max(case when product_code = 'TOTPRODS' and flow_code = 'CLOSTLV'  then value end) as product_stocks_kbbl
from b
group by country_code, period_date
