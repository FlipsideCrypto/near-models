{% docs defi__ez_lending %}

## Description
This table provides an enhanced view of all lending protocol transactions on the NEAR Protocol blockchain, primarily capturing activities on the Burrow lending protocol with token metadata, pricing information, and decimal adjustments. The table includes both raw and adjusted amounts, USD values, and comprehensive token context, making it the primary table for lending analytics and risk assessment. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for DeFi lending analysis and reporting.

## Key Use Cases
- Lending protocol volume analysis with accurate decimal-adjusted amounts and USD values
- Borrowing and lending pattern analysis and risk assessment with economic context
- Collateral utilization analysis and liquidation monitoring with value calculations
- Interest rate impact analysis and yield optimization
- Protocol health monitoring and risk metrics calculation
- User behavior analysis in lending protocols with value context
- Cross-protocol lending comparison and performance benchmarking

## Important Relationships
- Enhances `defi.fact_lending` with pricing and metadata from `price.ez_prices_hourly`
- Combines token metadata from `core.dim_ft_contract_metadata` for complete token context
- Supports `defi.ez_intents` with lending intent analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for lending metrics
- Provides foundation for all lending-related analytics and reporting
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `amount` and `amount_usd`: Essential for accurate lending volume analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `actions`: Critical for lending action classification and analysis
- `sender_id`: Important for user behavior analysis and lending patterns
- `platform`: Critical for protocol-specific analysis and comparison
- `symbol`: Important for token identification and lending analysis
- `token_address`: Essential for token-specific lending activity analysis

{% enddocs %} 