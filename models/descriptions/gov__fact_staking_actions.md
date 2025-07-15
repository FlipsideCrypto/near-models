{% docs gov__fact_staking_actions %}

## Description
This table contains all staking-related transactions on the NEAR Protocol blockchain, capturing staking actions such as deposits, withdrawals, and reward distributions. The data includes staking amounts, action types, and associated metadata, providing comprehensive tracking of staking participation and validator interactions. This table provides the foundation for staking analytics, validator performance analysis, and governance participation tracking across the NEAR ecosystem.

## Key Use Cases
- Staking participation analysis and user behavior tracking
- Validator performance analysis and staking pool comparison
- Staking reward analysis and yield optimization
- Staking pattern analysis and trend identification
- Cross-pool staking comparison and performance benchmarking
- Staking pool health monitoring and risk assessment
- Governance participation analysis and voting power tracking

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides staking data for `gov.ez_staking_actions` with enhanced metadata
- Supports `gov.fact_staking_pool_balances` with action impact analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for staking metrics
- Powers cross-pool analysis and validator performance comparison

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `action`: Critical for staking action classification and analysis
- `amount`: Important for staking volume analysis and participation tracking
- `address`: Essential for staking pool identification and analysis
- `signer_id`: Important for user behavior analysis and staking patterns
- `predecessor_id`: Useful for action flow tracking and context analysis

{% enddocs %} 