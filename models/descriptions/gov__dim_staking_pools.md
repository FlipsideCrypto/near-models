{% docs gov__dim_staking_pools %}

## Description
This table contains comprehensive metadata for staking pools on the NEAR Protocol blockchain, including pool addresses, owners, reward fee structures, and pool creation details. The data covers all staking pools that participate in NEAR's proof-of-stake consensus mechanism, providing essential context for staking analysis, validator performance tracking, and governance participation. This dimension table supports all staking-related analytics by providing standardized pool information and enabling proper staking pool categorization and analysis.

## Key Use Cases
- Staking pool identification and metadata enrichment
- Validator performance analysis and pool comparison
- Staking pool fee structure analysis and optimization
- Pool creation and lifecycle tracking
- Cross-pool performance benchmarking
- Staking pool discovery and participation analysis
- Governance participation tracking and analysis

## Important Relationships
- Enriches `gov.fact_staking_actions` with pool metadata and context
- Supports `gov.fact_staking_pool_balances` with pool identification
- Provides metadata for `gov.ez_staking_actions` with enhanced pool context
- Enables analysis in `stats.ez_core_metrics_hourly` for staking metrics
- Supports cross-pool analysis and validator performance comparison
- Powers governance analytics and staking participation analysis

## Commonly-used Fields
- `address`: Essential for joining with staking action and balance data
- `owner`: Critical for validator identification and ownership analysis
- `reward_fee_fraction`: Important for fee structure analysis and optimization
- `tx_hash`: Essential for pool creation transaction linking and verification
- `block_timestamp`: Primary field for time-series analysis and pool lifecycle tracking
- `tx_type`: Important for pool operation classification and analysis
- `inserted_timestamp`: Useful for pool discovery timeline analysis

{% enddocs %} 