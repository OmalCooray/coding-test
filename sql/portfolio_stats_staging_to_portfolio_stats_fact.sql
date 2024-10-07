INSERT INTO fact.portfolio_stats (business_date, portfolio_id, nav, daily_pnl, ytd_return, sharpe_ratio, volatility, var_95)
SELECT s.business_date, s.portfolio_id, s.nav, s.daily_pnl, s.ytd_return, s.sharpe_ratio, s.volatility, s.var_95
FROM (
    SELECT business_date, portfolio_id, 
           MAX(nav) AS nav,
           MAX(daily_pnl) AS daily_pnl,
           MAX(ytd_return) AS ytd_return,
           MAX(sharpe_ratio) AS sharpe_ratio,
           MAX(volatility) AS volatility,
           MAX(var_95) AS var_95
    FROM staging.portfolio_stats
    GROUP BY business_date, portfolio_id
) s
ON DUPLICATE KEY UPDATE
    nav = VALUES(nav),
    daily_pnl = VALUES(daily_pnl),
    ytd_return = VALUES(ytd_return),
    sharpe_ratio = VALUES(sharpe_ratio),
    volatility = VALUES(volatility),
    var_95 = VALUES(var_95);