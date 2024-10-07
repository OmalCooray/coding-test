  CREATE TABLE fact.portfolio_stats (
    business_date DATE NOT NULL,
    portfolio_id VARCHAR(255) NOT NULL,
    nav FLOAT,
    daily_pnl FLOAT,
    ytd_return FLOAT,
    sharpe_ratio FLOAT,
    volatility FLOAT,
    var_95 FLOAT,
    PRIMARY KEY (business_date, portfolio_id)
);