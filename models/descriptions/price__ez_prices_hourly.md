{% docs price__ez_prices_hourly %}

## Description
This table provides a comprehensive view of hourly price data for all assets on the NEAR Protocol blockchain, combining both native NEAR token prices and cross-chain bridged asset prices. The table includes token metadata, decimal precision, and price information, making it the primary table for price analytics and value calculations. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for price analysis, market monitoring, and economic calculations across the NEAR ecosystem.

## Key Use Cases
- Asset price monitoring and market trend analysis
- Token value calculations and USD conversion
- Cross-asset price comparison and correlation analysis
- Market volatility assessment and risk analysis
- Portfolio value tracking and performance calculation
- DeFi protocol value analysis and economic impact assessment
- Cross-chain asset price correlation and arbitrage analysis

## Important Relationships
- Combines data from `price.fact_prices_ohlc_hourly` with enhanced metadata
- Provides price data for all token-related models requiring USD values
- Supports `core.ez_token_transfers` with accurate price information
- Enables `defi.ez_dex_swaps` with swap value calculations
- Powers `defi.ez_bridge_activity` with cross-chain value analysis
- Supports `stats.ez_core_metrics_hourly` with price-based metrics
- Provides foundation for all economic analysis and value calculations

## Commonly-used Fields
- `hour`: Essential for time-series analysis and trend detection
- `price`: Critical for value calculations and economic analysis
- `token_address` and `symbol`: Important for asset identification and filtering
- `blockchain`: Useful for cross-chain price analysis and comparison
- `is_native`: Important for distinguishing native vs bridged assets
- `decimals`: Essential for accurate value calculations and decimal adjustment
- `is_deprecated` and `is_imputed`: Useful for data quality assessment

{% enddocs %} 