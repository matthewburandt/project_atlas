with deduped as (
    select *,
        row_number() over (partition by trade_date order by trade_date) as rn
    from {{ source('fund_data', 'etf_ohlc') }}
),

final as (
    select * except(rn)
    from deduped
    where rn = 1
)

select
    trade_date,

    -- GLD
    Open_GLD        as gld_open,
    High_GLD        as gld_high,
    Low_GLD         as gld_low,
    Close_GLD       as gld_close,
    Volume_GLD      as gld_volume,

    -- HYG
    Open_HYG        as hyg_open,
    High_HYG        as hyg_high,
    Low_HYG         as hyg_low,
    Close_HYG       as hyg_close,
    Volume_HYG      as hyg_volume,

    -- QQQ
    Open_QQQ        as qqq_open,
    High_QQQ        as qqq_high,
    Low_QQQ         as qqq_low,
    Close_QQQ       as qqq_close,
    Volume_QQQ      as qqq_volume,

    -- SPY
    Open_SPY        as spy_open,
    High_SPY        as spy_high,
    Low_SPY         as spy_low,
    Close_SPY       as spy_close,
    Volume_SPY      as spy_volume,

    -- TLT
    Open_TLT        as tlt_open,
    High_TLT        as tlt_high,
    Low_TLT         as tlt_low,
    Close_TLT       as tlt_close,
    Volume_TLT      as tlt_volume

from final