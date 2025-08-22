{% docs core__ez_token_transfers %}
This table records daily balances for NEAR Accounts, according to the parameters set by the Near team's balances indexer. More details around the source data can be found by reading about the NEAR Public Lakehouse on their documentation site: https://docs.near.org/tools/indexing.
  
Please Note - Flipside does not index or curate the data in this table, we simply provide it as-is from the NEAR Public Lakehouse with a daily sync from the public access table located at `bigquery-public-data.crypto_near_mainnet_us.ft_balances_daily`.
  
## Description
This table provides an enhanced view of all token transfers on the NEAR Protocol blockchain, combining raw transfer data with token metadata, pricing information, and decimal adjustments. The table includes both raw and adjusted amounts, USD values, and comprehensive token context, making it the primary table for token flow analysis and economic calculations. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for analytics, reporting, and business intelligence workflows.

## Key Use Cases
- Token flow analysis with accurate decimal-adjusted amounts and USD values
- DeFi protocol volume analysis and liquidity monitoring
- Token economics analysis and market impact assessment
- Cross-token comparison and portfolio analysis
- Whale movement tracking with value context
- Token distribution analysis and holder behavior tracking
- Economic impact analysis of token transfers and market movements

## Important Relationships
- Enhances `core.fact_token_transfers` with pricing and metadata from `price.ez_prices_hourly`
- Combines token metadata from `core.dim_ft_contract_metadata` for complete token context
- Supports `defi.ez_dex_swaps` with accurate token pricing and amounts
- Enables `defi.ez_bridge_activity` with cross-chain value analysis
- Powers `stats.ez_core_metrics_hourly` with aggregated transfer values
- Provides foundation for all token-related analytics and reporting

## Commonly-used Fields
- `amount_raw` and `amount_precise`: Essential for accurate token amount analysis
- `amount_usd`: Critical for economic analysis and value calculations
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `from_address` and `to_address`: Important for flow analysis and network mapping
- `contract_address`: Essential for token-specific analysis and filtering
- `symbol` and `decimals`: Important for token identification and decimal handling
- `token_price`: Useful for price trend analysis and market impact assessment

{% enddocs %} 