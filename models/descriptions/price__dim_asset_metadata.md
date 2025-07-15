{% docs price__dim_asset_metadata %}

## Description
This table contains comprehensive metadata for all assets tracked for pricing on the NEAR Protocol blockchain, including token addresses, symbols, names, and blockchain platform information. The data covers both native NEAR tokens and cross-chain bridged assets, providing essential context for price identification, asset categorization, and multi-chain price correlation. This dimension table supports all price-related analytics by providing standardized asset information and enabling proper price data organization and analysis.

## Key Use Cases
- Asset identification and price data correlation
- Cross-chain asset mapping and price comparison
- Multi-provider price data integration and validation
- Asset categorization and platform-specific analysis
- Price data quality assessment and metadata validation
- Cross-asset price correlation and market analysis
- Asset discovery and price coverage analysis

## Important Relationships
- Enriches `price.fact_prices_ohlc_hourly` with asset metadata and identification
- Supports `price.ez_prices_hourly` with enhanced asset context
- Provides metadata for `price.ez_asset_metadata` with comprehensive asset information
- Enables analysis in `stats.ez_core_metrics_hourly` for price metrics
- Supports cross-asset analysis and multi-chain price correlation

## Commonly-used Fields
- `token_address`: Essential for joining with price data and asset identification
- `symbol` and `name`: Critical for human-readable asset identification
- `platform` and `platform_id`: Important for blockchain-specific analysis
- `provider`: Useful for price data source identification and reliability assessment
- `asset_id`: Important for unique asset identification across systems
- `inserted_timestamp`: Useful for asset discovery timeline analysis

{% enddocs %} 