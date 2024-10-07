INSERT INTO fact.holdings_data (business_date, portfolio_id, security_id, exchange, quantity, market_value, currency)
SELECT s.business_date, s.portfolio_id, s.security_id, s.exchange, s.quantity, s.market_value, s.currency
FROM (
    SELECT business_date, portfolio_id, security_id,
           MAX(exchange) AS exchange,
           MAX(quantity) AS quantity,
           MAX(market_value) AS market_value,
           MAX(currency) AS currency
    FROM staging.holdings_data
    GROUP BY business_date, portfolio_id, security_id
) s
ON DUPLICATE KEY UPDATE
    exchange = VALUES(exchange),
    quantity = VALUES(quantity),
    market_value = VALUES(market_value),
    currency = VALUES(currency);