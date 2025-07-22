{% docs core__fact_blocks %}

## Description
This table contains all finalized blocks on the NEAR Protocol blockchain, providing comprehensive block-level metadata including block height, timestamp, author (validator), cryptographic hashes, and protocol-specific header fields. Each record represents a unique block, capturing the full context of NEAR's sharded architecture, including chunk and epoch data, validator proposals, and protocol versioning. The table is sourced from the final, canonical chain state and is foundational for all temporal, transactional, and consensus analytics on NEAR. It exposes both raw and parsed header fields, supporting deep analysis of block production, validator behavior, and protocol evolution.

## Key Use Cases
- Block production analysis and validator performance tracking
- Network activity monitoring and chain growth analysis
- Temporal alignment for transactions, receipts, and logs
- Consensus and protocol upgrade tracking
- Sharding and chunk distribution analysis
- Economic analysis of total supply, gas price, and validator rewards
- Cross-referencing with transaction, receipt, and event tables for full blockchain context

## Important Relationships
- Links to `core.fact_transactions` and `core.fact_receipts` through `block_id` for temporal and execution context
- Provides block context for `core.fact_logs`, `core.fact_token_transfers`, and all downstream analytics tables
- Supports `stats.ez_core_metrics_hourly` for aggregated block and network metrics
- Enables sharding and chunk analysis via chunk-related fields and relationships to chunk-level tables
- Serves as the temporal backbone for all gold layer models in the NEAR analytics suite

## Commonly-used Fields
- `block_id`: Essential for unique block identification and joining with all transactional tables
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `block_hash`: Critical for block verification and chain integrity
- `block_author`: Important for validator analysis and block production tracking
- `tx_count`: Key for network activity and throughput analysis
- `header`, `chunks`, and chunk-related fields: Support sharding, chunk distribution, and protocol-level analytics
- `total_supply`, `gas_price`, `validator_proposals`, `validator_reward`: Important for economic and protocol analysis
- `inserted_timestamp`, `modified_timestamp`: Useful for data freshness and ETL monitoring

{% enddocs %} 