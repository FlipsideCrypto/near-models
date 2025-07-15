{% docs core__fact_transactions %}

## Description
This table contains all transactions processed on the NEAR Protocol blockchain, providing comprehensive transaction metadata including signer and receiver information, gas consumption, fees, and execution status. The data captures the complete transaction lifecycle from submission to finalization, including cryptographic signatures, nonce values, and gas mechanics. This table serves as the primary fact table for transaction-level analytics and provides the foundation for understanding user activity, contract interactions, and network utilization patterns.

## Key Use Cases
- Transaction volume analysis and network activity monitoring
- User behavior analysis and wallet activity tracking
- Gas consumption analysis and fee optimization studies
- Contract interaction analysis and smart contract usage patterns
- Transaction success rate analysis and failure investigation
- Cross-account transaction flow analysis and network mapping
- Economic analysis of transaction fees and gas pricing

## Important Relationships
- Links to `core.fact_blocks` through block_id for temporal and block context
- Connects to `core.fact_receipts` through tx_hash for execution outcome details
- Provides transaction context for `core.fact_logs` and `core.fact_token_transfers`
- Supports `core.ez_actions` for detailed action-level analysis
- Enables analysis in `defi.ez_dex_swaps` and `defi.ez_bridge_activity` through transaction context
- Powers `stats.ez_core_metrics_hourly` for aggregated transaction metrics

## Commonly-used Fields
- `tx_hash`: Essential for unique transaction identification and blockchain verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `tx_signer` and `tx_receiver`: Critical for user activity analysis and network mapping
- `gas_used` and `transaction_fee`: Important for economic analysis and cost optimization
- `tx_succeeded`: Essential for success rate analysis and failure investigation
- `attached_gas`: Important for understanding gas mechanics and transaction planning
- `nonce`: Critical for transaction ordering and replay protection analysis

{% enddocs %} 