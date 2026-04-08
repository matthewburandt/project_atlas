select
    trade_date,
    Spread_2Y_10Y               as spread_2y_10y,
    Fed_Funds_Rate              as fed_funds_rate,
    High_Yield_Spread           as high_yield_spread,
    VIX                         as vix,
    Inflation_Expectations_10Y  as inflation_expectations_10y,
    CPI                         as cpi,
    Unemployment_Rate           as unemployment_rate

from {{ source('macro_data', 'macro_data') }}