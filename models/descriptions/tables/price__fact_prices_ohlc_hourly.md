{% docs price__fact_prices_ohlc_hourly %}

## Description
This table contains hourly OHLC (Open, High, Low, Close) price data for all tracked assets on the NEAR Protocol blockchain, capturing price movements and volatility patterns across different time periods. The data includes comprehensive price information from multiple providers, enabling detailed market analysis, volatility assessment, and price trend identification. This table provides the foundation for price analytics, market analysis, and economic calculations across the NEAR ecosystem.

## Key Use Cases
- Asset price trend analysis and market movement tracking
- Volatility analysis and risk assessment
- Price correlation analysis across different assets
- Market timing analysis and trading pattern identification
- Cross-provider price validation and data quality assessment
- Economic impact analysis of price movements
- Portfolio performance tracking and value calculations

## Important Relationships
- Links to `price.dim_asset_metadata` through asset_id for asset identification
- Provides price data for `price.ez_prices_hourly` with enhanced metadata
- Supports `price.ez_asset_metadata` with comprehensive price context
- Enables analysis in `stats.ez_core_metrics_hourly` for price metrics
- Powers value calculations across all token-related models
- Supports cross-asset analysis and market correlation studies

## Commonly-used Fields
- `hour`: Essential for time-series analysis and trend detection
- `open`, `high`, `low`, `close`: Critical for price movement analysis and volatility calculation
- `asset_id`: Important for asset-specific price analysis and correlation
- `provider`: Useful for price data source identification and reliability assessment
- `inserted_timestamp`: Important for data freshness and update tracking
- `fact_prices_ohlc_hourly_id`: Essential for unique price record identification

{% enddocs %} 