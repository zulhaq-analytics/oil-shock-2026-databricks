-- Fails if any oil price is zero or negative.
-- WTI fell below zero on 2020-04-20 (spot and futures); those two days are allowed.
select *
from {{ ref('fct_price_daily') }}
where price_group in ('crude', 'product', 'crude_futures')
  and price <= 0
  and price_date <> to_date('2020-04-20')
