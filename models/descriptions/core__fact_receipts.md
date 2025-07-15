{% docs core__fact_receipts %}

## Description
This table contains all receipt execution outcomes on the NEAR Protocol blockchain, capturing the results of transaction processing including gas consumption, execution status, logs, and cross-contract communication details. Receipts represent the execution units in NEAR's sharding architecture and contain the actual state changes, function calls, and event emissions that occur during transaction processing. This table provides the foundation for understanding smart contract execution, cross-shard communication, and the complete transaction lifecycle from submission to final state changes.

## Key Use Cases
- Smart contract execution analysis and performance monitoring
- Cross-contract call tracking and dependency analysis
- Gas consumption analysis and optimization studies
- Execution failure investigation and error pattern analysis
- Cross-shard transaction flow analysis and sharding efficiency
- Event log analysis and contract interaction patterns
- State change tracking and account balance analysis

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides execution context for `core.fact_logs` and `core.fact_token_transfers`
- Supports `core.ez_actions` for detailed action-level analysis
- Enables analysis in `defi.ez_dex_swaps` and `defi.ez_bridge_activity` through receipt outcomes
- Powers `stats.ez_core_metrics_hourly` for aggregated execution metrics

## Commonly-used Fields
- `receipt_id`: Essential for unique receipt identification and execution tracking
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `receiver_id` and `predecessor_id`: Critical for cross-contract call analysis and flow tracking
- `gas_burnt`: Important for gas consumption analysis and cost optimization
- `receipt_succeeded`: Essential for success rate analysis and failure investigation
- `logs`: Critical for event analysis and contract interaction patterns
- `outcome`: Important for detailed execution analysis and state change tracking

{% enddocs %} 