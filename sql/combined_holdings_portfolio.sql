CREATE VIEW fact.combined_holdings_portfolio AS
SELECT
    h.business_date,
    h.portfolio_id,
    h.security_id,
    h.exchange,
    h.quantity,
    h.market_value,
    h.currency,
    p.nav,
    p.daily_pnl,
    p.ytd_return,
    p.sharpe_ratio,
    p.volatility,
    p.var_95
FROM
    fact.holdings_data h
JOIN
    fact.portfolio_stats p
ON
    h.business_date = p.business_date
    AND h.portfolio_id = p.portfolio_id;