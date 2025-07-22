{% docs gov__fact_staking_pool_balances %}

## Description
This table contains all staking pool balance changes on the NEAR Protocol blockchain, capturing the dynamic balance updates of staking pools as tokens are deposited, withdrawn, or rewards are distributed. The data includes balance amounts, pool addresses, and associated metadata, providing comprehensive tracking of staking pool liquidity and performance. This table provides the foundation for staking pool analytics, validator performance analysis, and pool health monitoring across the NEAR ecosystem.

## Key Use Cases
- Staking pool balance analysis and liquidity monitoring
- Validator performance tracking and pool comparison
- Staking pool health assessment and risk analysis
- Balance change pattern analysis and trend identification
- Cross-pool balance comparison and performance benchmarking
- Staking pool growth analysis and adoption tracking
- Governance participation analysis and voting power distribution

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides balance data for `gov.fact_staking_pool_daily_balances` with aggregated metrics
- Supports `gov.fact_staking_actions` with balance impact analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for staking metrics
- Powers cross-pool analysis and validator performance comparison

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `balance`: Critical for pool balance analysis and liquidity assessment
- `address`: Essential for staking pool identification and analysis
- `receipt_object_id`: Important for action-level analysis and receipt context
- `block_id`: Useful for temporal ordering and block-level analysis
- `inserted_timestamp`: Important for data freshness and update tracking

{% enddocs %} 