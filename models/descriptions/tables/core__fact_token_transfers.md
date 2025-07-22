{% docs core__fact_token_transfers %}

## Description
This table contains all token transfers on the NEAR Protocol blockchain, capturing movements of native NEAR tokens, fungible tokens (NEP-141), and other token types across accounts and contracts. The data includes both direct transfers and complex token operations like minting, deposits, and liquidity operations. This table provides the foundation for understanding token economics, user behavior, and DeFi activity across the NEAR ecosystem, supporting comprehensive token flow analysis and balance tracking.

## Key Use Cases
- Token flow analysis and whale movement tracking
- DeFi protocol volume analysis and liquidity monitoring
- Token distribution analysis and holder behavior tracking
- Cross-contract token movement analysis
- Economic analysis of token transfers and value flows
- Compliance and audit trail analysis for token movements
- Token minting and burning pattern analysis

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides transfer data for `core.ez_token_transfers` with enhanced metadata
- Supports `defi.ez_dex_swaps` and `defi.ez_bridge_activity` through transfer events
- Enables analysis in `nft.ez_nft_sales` through token payment flows
- Powers `stats.ez_core_metrics_hourly` for aggregated transfer metrics

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `from_address` and `to_address`: Critical for flow analysis and network mapping
- `contract_address`: Important for token-specific analysis and contract tracking
- `amount_unadj`: Essential for value calculations and economic analysis
- `transfer_type`: Important for categorizing different token standards
- `transfer_action`: Critical for understanding the specific operation type

{% enddocs %} 