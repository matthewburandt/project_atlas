with spine as (
    -- generate one row per calendar date between first and last macro observation
    select trade_date
    from unnest(
        generate_date_array(
            (select min(trade_date) from {{ ref('stg_macro_data') }}),
            (select max(trade_date) from {{ ref('stg_macro_data') }})
        )
    ) as trade_date
),

macro_with_spine as (
    select
        spine.trade_date,
        macro.spread_2y_10y,
        macro.fed_funds_rate,
        macro.high_yield_spread,
        macro.vix,
        macro.inflation_expectations_10y,
        macro.cpi,
        macro.unemployment_rate
    from spine
    left join {{ ref('stg_macro_data') }} as macro
        on spine.trade_date = macro.trade_date
),

forward_filled as (
    select
        trade_date,

        -- raw values, forward filled
        last_value(spread_2y_10y ignore nulls)          over w as spread_2y_10y,
        last_value(fed_funds_rate ignore nulls)         over w as fed_funds_rate,
        last_value(high_yield_spread ignore nulls)      over w as high_yield_spread,
        last_value(vix ignore nulls)                    over w as vix,
        last_value(inflation_expectations_10y ignore nulls) over w as inflation_expectations_10y,
        last_value(cpi ignore nulls)                    over w as cpi,
        last_value(unemployment_rate ignore nulls)      over w as unemployment_rate

    from macro_with_spine
    window w as (
        order by trade_date
        rows between unbounded preceding and current row
    )
),

with_features as (
    select
        trade_date,

        -- raw values
        spread_2y_10y,
        fed_funds_rate,
        high_yield_spread,
        vix,
        inflation_expectations_10y,
        cpi,
        unemployment_rate,

        -- month-over-month changes
        spread_2y_10y - lag(spread_2y_10y, 21)         over (order by trade_date) as spread_2y_10y_mom,
        fed_funds_rate - lag(fed_funds_rate, 21)        over (order by trade_date) as fed_funds_rate_mom,
        high_yield_spread - lag(high_yield_spread, 21)  over (order by trade_date) as high_yield_spread_mom,
        vix - lag(vix, 21)                              over (order by trade_date) as vix_mom,
        inflation_expectations_10y - lag(inflation_expectations_10y, 21) over (order by trade_date) as inflation_expectations_10y_mom,
        cpi - lag(cpi, 21)                              over (order by trade_date) as cpi_mom,
        unemployment_rate - lag(unemployment_rate, 21)  over (order by trade_date) as unemployment_rate_mom

    from forward_filled
),

with_trend as (
    select
        *,

        -- trend direction (threshold of 0.05 = flat zone, adjust as needed)
        case
            when spread_2y_10y_mom > 0.05           then 'up'
            when spread_2y_10y_mom < -0.05          then 'down'
            else 'flat'
        end as spread_2y_10y_trend,

        case
            when fed_funds_rate_mom > 0.05          then 'up'
            when fed_funds_rate_mom < -0.05         then 'down'
            else 'flat'
        end as fed_funds_rate_trend,

        case
            when high_yield_spread_mom > 0.05       then 'up'
            when high_yield_spread_mom < -0.05      then 'down'
            else 'flat'
        end as high_yield_spread_trend,

        case
            when vix_mom > 0.05                     then 'up'
            when vix_mom < -0.05                    then 'down'
            else 'flat'
        end as vix_trend,

        case
            when inflation_expectations_10y_mom > 0.05  then 'up'
            when inflation_expectations_10y_mom < -0.05 then 'down'
            else 'flat'
        end as inflation_expectations_10y_trend,

        case
            when cpi_mom > 0.05                     then 'up'
            when cpi_mom < -0.05                    then 'down'
            else 'flat'
        end as cpi_trend,

        case
            when unemployment_rate_mom > 0.05       then 'up'
            when unemployment_rate_mom < -0.05      then 'down'
            else 'flat'
        end as unemployment_rate_trend

    from with_features
)

select * from with_trend