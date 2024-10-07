CREATE TABLE fact.holdings_data (
    business_date DATE NOT NULL,
    portfolio_id VARCHAR(255) NOT NULL,
    security_id VARCHAR(255) NOT NULL,
    exchange ENUM('NASDAQ', 'NYSE'),
    quantity INT NOT NULL,
    market_value FLOAT NOT NULL,
    currency VARCHAR(255),
    PRIMARY KEY (business_date, portfolio_id, security_id)
);