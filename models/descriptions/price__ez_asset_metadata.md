{% docs price__ez_asset_metadata %}

## Description
This table provides a comprehensive view of asset metadata for all tracked assets on the NEAR Protocol blockchain, combining both native NEAR token metadata and cross-chain bridged asset metadata. The table includes token addresses, symbols, names, decimal precision, and blockchain platform information, making it the primary table for asset identification and categorization. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for asset analysis and metadata enrichment across the NEAR ecosystem.

## Key Use Cases
- Asset identification and metadata enrichment for analytics
- Cross-chain asset mapping and correlation analysis
- Token categorization and platform-specific analysis
- Asset discovery and coverage analysis
- Multi-provider metadata validation and quality assessment
- Cross-asset comparison and ecosystem mapping
- Asset standardization and metadata quality improvement

## Important Relationships
- Combines data from `price.dim_asset_metadata` with enhanced categorization
- Provides metadata for `price.ez_prices_hourly` with comprehensive asset context
- Supports `core.ez_token_transfers` with enhanced token identification
- Enables `defi.ez_dex_swaps` with token pair analysis
- Powers `defi.ez_bridge_activity` with cross-chain asset mapping
- Supports `stats.ez_core_metrics_hourly` with asset-based metrics
- Provides foundation for all asset-related analytics and reporting

## Commonly-used Fields
- `token_address`: Essential for joining with transaction and price data
- `symbol` and `name`: Critical for human-readable asset identification
- `blockchain`: Important for cross-chain analysis and platform comparison
- `is_native`: Essential for distinguishing native vs bridged assets
- `decimals`: Important for accurate value calculations and decimal adjustment
- `asset_id`: Useful for unique asset identification across systems
- `is_deprecated`: Important for data quality assessment and asset lifecycle tracking

{% enddocs %} 