CREATE TABLE staging.holdings_data (
    business_date DATE NOT NULL,
    portfolio_id VARCHAR(255) NOT NULL,
    security_id VARCHAR(255) NOT NULL,
    exchange ENUM('NASDAQ', 'NYSE'),
    quantity INT NOT NULL,
    market_value FLOAT NOT NULL,
    currency VARCHAR(255)
);