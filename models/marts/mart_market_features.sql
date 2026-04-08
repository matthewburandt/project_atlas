select
    f.*,
    m.* except(trade_date)
from {{ ref('int_fund_daily') }} f
left join {{ ref('int_macro_daily') }} m
    on f.trade_date = m.trade_date