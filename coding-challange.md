# Coding Challenge: Investment Portfolio Data ETL Pipeline

## Background

The goal of this challenge is to build a flexible, new data pipeline for crucial investment data. The source of the pipeline is csv files, and the sink is a SQL database used by the investment team for portfolio analysis.

## Deliverable

The deliverable for this challenge is a folder or git repository containing all source code and related files.

## Requirements

1. Implement the following either as methods of a class or as standalone functions:
   - `extract_data_from_csv`
   - `transform_data_in_python`
   - `upload_data_to_sql`
   - `transform_data_in_sql`

2. The above methods/functions should be able to:
   - Handle CSV files with varying levels of data completeness
   - Clean the data and fit the full column schema before loading into SQL, handling missing columns appropriately. Data cleaning should include:
     - Converting numeric-like fields to numeric data types
     - Stripping white space and quotation characters
     - Convert dates to a native date field
   - Connect to any SQL database
   - Upload data into SQL staging tables
   - Perform the following in-SQL transformations
     - Move the data from staging tables to final tables, with proper data types
     - Create a view that combines holdings and portfolio data
   
3. You will be working with two types of CSV files:
   - End of day portfolio holdings (1 row = 1 security holding)
   - Portfolio-level P&L, exposure, and risk stats (1 row = 1 rolled-up portfolio)

4. Include any necessary environment definition files (e.g., requirements.txt, pyproject.toml) required to run this code.

## Sample Data

holdings_data_1.csv:
```
business_date,portfolio_id,security_id,exchange,quantity,market_value,currency
2023-06-30,PORT001,AAPL,NASDAQ,1000,189523.50,USD
2023-06-30,PORT001,GOOGL,NASDAQ,500,61234.75,USD
2023-06-30,PORT001,MSFT,NASDAQ,750,253987.25,USD
2023-06-30,PORT001,AMZN,NASDAQ,300,34998.00,USD
2023-06-30,PORT002,TSLA,NASDAQ,200.0,49876.00,USD
2023-06-30,PORT002,NVDA,NASDAQ,400,181234.00,USD
2023-06-30,PORT002 ,AMD,NASDAQ,1000,109876.50,USD
2023-06-30,PORT003,JPM,NYSE,600,79998.00,USD
2023-06-30,PORT003,BAC,NYSE,2000,59876.00,USD
06/30/2023,PORT003,WFC,NYSE,1500,64987.50,USD
2023-06-30,PORT004,JNJ,NYSE,400,70123.00,USD
2023-06-30,PORT004,PFE,NYSE,1000,39998.00,USD
2023-06-30,PORT004,MRK,NYSE,800,90234.00,USD
30-06-2023,PORT005,XOM,NYSE,700,74998.50,USD
2023-06-30,PORT005,CVX,NYSE,500,54987.25,USD
```

holdings_data_2.csv:
```
business_date,portfolio_id,security_id,quantity,market_value
2023-06-30,PORT001,AAPL,1000,"189,523.50"
2023-06-30,PORT001,GOOGL,500,"61,234.75"
2023-06-30,PORT001,MSFT,750,"253,987.25"
2023-06-30,PORT001,AMZN,300,"34,998.00"
2023-06-30,PORT002,TSLA,200,"49,876.00"
2023-06-30,PORT002,NVDA ,400,"181,234.00"
2023-06-30,PORT002,AMD,1000,109876.50
2023-06-30,PORT003,JPM,600,"79,998.00"
2023-06-30,PORT003,BAC,2000,"59,876.00"
2023-06-30,PORT003,WFC,1500,"64,987.50"
2023-06-30,PORT004,JNJ,400,70123
2023-06-30, PORT004,PFE,1000,"39,998.00"
2023-06-30,PORT004,MRK,800,"90,234.00"
2023-06-30,PORT005,XOM,700,"74,998.50"
2023-06-30,PORT005,CVX,500,"54,987.25"
```

portfolio_stats_1.csv:
```
business_date,portfolio_id,nav,daily_pnl,ytd_return,sharpe_ratio,volatility,var_95
2023-06-30,PORT001,10234567.89,52345.67,0.0768,1.23,0.1487,453678.90
2023-06-30,PORT002,20123456.78,98765.43,0.0932,1.41,0.1789,798765.43
2023-06-30,PORT003,15345678.90,73456.78,0.0654,1.12,0.1398,601234.56 
2023-06-30,PORT004,12456789.01,61234.56,0.0843,1.31,0.1576,499876.54
 2023-06-30,PORT005,"18,234,567.89",87654.32,0.0721,1.19,0.1687,698765.43
06/29/2023,PORT001,9987654.32,41234.56,0.0763,1.22,0.1492,448765.43
2023-06-29,PORT002,19876543.21,94321.09,0.0927,1.40,0.1798,796543.21
29-06-2023,PORT003,14987654.32,69876.54,0.0649,1.11,0.1401,597654.32 
2023-06-29,PORT004,11987654.32,56789.01,0.0838,1.30,0.1581,496543.21
2023-06-29,PORT005,17987654.32,83210.98,0.0716,1.18,0.1692,694321.09
```

portfolio_stats_2.csv:
```
business_date,portfolio_id,nav,daily_pnl,ytd_return
2023-06-30,PORT001,10234567.89,52345.67,7.68%
2023-06-30,PORT002,20123456.78,98765.43,9.32%
2023-06-30,PORT003,15345678.90,73456.78,6.54%
2023-06-30,PORT004,12456789.01,61234.56,8.43%
2023-06-30,PORT005,18234567.89,87654.32,0.0721
2023-06-29,PORT001,9987654.32,41234.56,7.63% 
2023-06-29,PORT002,19876543.21,94321.09,9.27%
2023-06-29,PORT003,14987654.32,69876.54,6.49%
2023-06-29,PORT004,11987654.32,56789.01,8.38%
2023-06-29,PORT005,17987654.32,83210.98,7.16%
```

## Evaluation Criteria
- How comfortable would we feel maintaining, debugging, and running this code in production?
- For possible follow-up discussion: The solution must meet the requirements, but we're equally interested in how you architected the solution and why. What are the design choices that you rejected? If you had more time, how might you change the design? If this pipeline had to scale to much larger file sizes and many more file types, how would you evolve your code?

## Hints
- The use of any and all developer tools, including AI-based tools (like Claude.ai or ChatGPT), is welcomed.
- The use of additional user-defined objects to support the output is encouraged to support a modular and extensible design.