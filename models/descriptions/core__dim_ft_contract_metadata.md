{% docs core__dim_ft_contract_metadata %}

## Description
This table contains comprehensive metadata for fungible token contracts on the NEAR Protocol blockchain, including token names, symbols, decimal precision, and cross-chain mapping information. The data covers both native NEAR tokens and cross-chain bridged assets, providing essential context for token identification, pricing, and value calculations. This dimension table supports all token-related analytics by providing standardized token information and enabling proper decimal adjustment for accurate value analysis.

## Key Use Cases
- Token identification and metadata enrichment for transfer analysis
- Cross-chain token correlation and bridge activity analysis
- Token pricing and market data integration
- Decimal adjustment for accurate value calculations
- Token supply and economics analysis
- Multi-chain portfolio tracking and analysis
- Token standard compliance and verification

## Important Relationships
- Enriches `core.fact_token_transfers` with token metadata and decimal information
- Supports `core.ez_token_transfers` with enhanced token context
- Enables `defi.ez_dex_swaps` with token pair identification and pricing
- Powers `defi.ez_bridge_activity` with cross-chain token mapping
- Supports `price.ez_prices_hourly` with token metadata for price analysis
- Enables accurate USD value calculations across all token-related models

## Commonly-used Fields
- `contract_address`: Essential for joining with transfer and swap data
- `name` and `symbol`: Critical for human-readable token identification
- `decimals`: Important for accurate value calculations and decimal adjustment
- `source_chain`: Essential for cross-chain analysis and bridge tracking
- `crosschain_token_contract`: Important for multi-chain token correlation
- `asset_identifier`: Useful for unique token identification across systems

{% enddocs %} 