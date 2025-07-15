{% docs defi__ez_dex_swaps %}

## Description
This table provides an enhanced view of all decentralized exchange (DEX) swap transactions on the NEAR Protocol blockchain, combining raw swap data with token metadata, pricing information, and decimal adjustments. The table includes both raw and adjusted amounts, USD values, and comprehensive token context, making it the primary table for DEX analytics and trading analysis. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for DeFi analytics, reporting, and business intelligence workflows.

## Key Use Cases
- DEX trading volume analysis with accurate decimal-adjusted amounts and USD values
- Token pair liquidity analysis and trading pattern identification
- Price impact analysis and slippage calculation with USD context
- DEX protocol comparison and performance benchmarking
- Arbitrage opportunity detection and analysis
- Trading bot activity monitoring and pattern recognition
- DeFi protocol integration analysis and cross-protocol flows

## Important Relationships
- Enhances `defi.fact_dex_swaps` with pricing and metadata from `price.ez_prices_hourly`
- Combines token metadata from `core.dim_ft_contract_metadata` for complete token context
- Supports `defi.ez_intents` with swap execution analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for DeFi metrics
- Provides foundation for all DEX-related analytics and reporting
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `amount_in` and `amount_out`: Essential for accurate swap amount analysis
- `amount_in_usd` and `amount_out_usd`: Critical for economic analysis and value calculations
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `trader`: Important for user behavior analysis and trading patterns
- `platform`: Essential for DEX protocol analysis and comparison
- `symbol_in` and `symbol_out`: Important for token pair identification and analysis
- `pool_id`: Useful for liquidity pool analysis and performance tracking

{% enddocs %} 