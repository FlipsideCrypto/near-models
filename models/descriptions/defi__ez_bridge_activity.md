{% docs defi__ez_bridge_activity %}

## Description
This table provides an enhanced view of all cross-chain bridge transactions on the NEAR Protocol blockchain, combining raw bridge data with token metadata, pricing information, and cross-chain mapping details. The table includes both raw and adjusted amounts, USD values, and comprehensive token context across multiple bridge protocols including Rainbow Bridge, Wormhole, Multichain, Allbridge, and Omni. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for cross-chain analytics and bridge analysis.

## Key Use Cases
- Cross-chain bridge volume analysis with accurate decimal-adjusted amounts and USD values
- Bridge protocol comparison and performance benchmarking with value context
- Cross-chain token flow analysis and liquidity tracking
- Bridge security analysis and risk assessment with economic impact
- Multi-chain portfolio tracking and analysis
- Bridge protocol adoption and user behavior analysis
- Cross-chain arbitrage opportunity detection with value calculations

## Important Relationships
- Enhances `defi.fact_bridge_activity` with pricing and metadata from `price.ez_prices_hourly`
- Combines token metadata from `core.dim_ft_contract_metadata` for complete token context
- Enables analysis in `stats.ez_core_metrics_hourly` for bridge metrics
- Provides foundation for all cross-chain analytics and reporting
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `amount` and `amount_usd`: Essential for accurate bridge volume analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `source_chain` and `destination_chain`: Critical for cross-chain flow analysis
- `platform`: Important for bridge protocol comparison and analysis
- `direction`: Critical for understanding bridge flow direction (inbound/outbound)
- `symbol`: Important for token identification and cross-chain mapping
- `token_address`: Essential for token-specific bridge activity analysis

{% enddocs %} 