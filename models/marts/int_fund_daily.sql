select
    trade_date,

    -- GLD
    -- Daily return: how much did the price change vs. yesterday?
    safe_divide(gld_close - lag(gld_close) over (order by trade_date), 
                lag(gld_close) over (order by trade_date)) as gld_daily_return,

    -- Normalized interday range: how wide was the day's range relative to close?
    safe_divide(gld_high - gld_low, gld_close) as gld_interday_range,
    gld_open,
    gld_high,
    gld_low,
    gld_close,
    gld_volume,

    -- HYG
    -- Daily return: how much did the price change vs. yesterday?
    safe_divide(hyg_close - lag(hyg_close) over (order by trade_date), 
                lag(hyg_close) over (order by trade_date)) as hyg_daily_return,

    -- Normalized interday range: how wide was the day's range relative to close?
    safe_divide(hyg_high - hyg_low, hyg_close) as hyg_interday_range,
    hyg_open,
    hyg_high,
    hyg_low,
    hyg_close,
    hyg_volume,

    -- QQQ
    -- Daily return: how much did the price change vs. yesterday?
    safe_divide(qqq_close - lag(qqq_close) over (order by trade_date), 
                lag(qqq_close) over (order by trade_date)) as qqq_daily_return,

    -- Normalized interday range: how wide was the day's range relative to close?
    safe_divide(qqq_high - qqq_low, qqq_close) as qqq_interday_range,
    qqq_open,
    qqq_high,
    qqq_low,
    qqq_close,
    qqq_volume,

    -- SPY
    -- Daily return: how much did the price change vs. yesterday?
    safe_divide(spy_close - lag(spy_close) over (order by trade_date), 
                lag(spy_close) over (order by trade_date)) as spy_daily_return,

    -- Normalized interday range: how wide was the day's range relative to close?
    safe_divide(spy_high - spy_low, spy_close) as spy_interday_range,
    spy_open,
    spy_high,
    spy_low,
    spy_close,
    spy_volume,
    
    -- TLT
    -- Daily return: how much did the price change vs. yesterday?
    safe_divide(tlt_close - lag(tlt_close) over (order by trade_date), 
                lag(tlt_close) over (order by trade_date)) as tlt_daily_return,

    -- Normalized interday range: how wide was the day's range relative to close?
    safe_divide(tlt_high - tlt_low, tlt_close) as tlt_interday_range,
    tlt_open,
    tlt_high,
    tlt_low,
    tlt_close,
    tlt_volume

from {{ ref('stg_fund_data') }}