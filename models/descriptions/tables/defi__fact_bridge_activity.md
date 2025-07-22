{% docs defi__fact_bridge_activity %}

## Description
This table contains all cross-chain bridge transactions on the NEAR Protocol blockchain, capturing token movements between NEAR and other blockchains through various bridge protocols including Rainbow Bridge, Wormhole, Multichain, Allbridge, and Omni. The data includes bridge-specific details such as source/destination chains, token addresses, amounts, and bridge protocol metadata. This table provides the foundation for cross-chain analytics, bridge volume analysis, and multi-chain ecosystem monitoring.

## Key Use Cases
- Cross-chain bridge volume analysis and flow monitoring
- Bridge protocol comparison and performance benchmarking
- Cross-chain token flow analysis and liquidity tracking
- Bridge security analysis and risk assessment
- Multi-chain portfolio tracking and analysis
- Bridge protocol adoption and user behavior analysis
- Cross-chain arbitrage opportunity detection

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides bridge data for `defi.ez_bridge_activity` with enhanced metadata
- Supports `defi.ez_intents` with cross-chain intent analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for bridge metrics
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `source_chain` and `destination_chain`: Critical for cross-chain flow analysis
- `token_address`: Important for token-specific bridge activity analysis
- `amount_raw` and `amount_adj`: Essential for bridge volume calculations
- `platform`: Important for bridge protocol comparison and analysis
- `direction`: Critical for understanding bridge flow direction (inbound/outbound)

{% enddocs %} 