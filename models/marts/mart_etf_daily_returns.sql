with source as (
    select * from {{ ref('mart_market_features') }}  -- swap for your actual mart model name
),

unpivoted as (
    select trade_date, 'GLD' as ticker, gld_daily_return as daily_return_pct from source
    union all
    select trade_date, 'HYG' as ticker, hyg_daily_return as daily_return_pct from source
    union all
    select trade_date, 'QQQ' as ticker, qqq_daily_return as daily_return_pct from source
    union all
    select trade_date, 'SPY' as ticker, spy_daily_return as daily_return_pct from source
    union all
    select trade_date, 'TLT' as ticker, tlt_daily_return as daily_return_pct from source
)

select * from unpivoted
where daily_return_pct is not null