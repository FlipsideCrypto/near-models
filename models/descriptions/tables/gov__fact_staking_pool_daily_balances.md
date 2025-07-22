{% docs gov__fact_staking_pool_daily_balances %}

## Description
This table contains daily balance snapshots for all staking pools on the NEAR Protocol blockchain, providing aggregated daily balance data for staking pool analysis and performance tracking. The data includes daily balance amounts, pool addresses, and associated metadata, enabling historical analysis of staking pool growth, performance trends, and liquidity patterns. This table provides the foundation for staking pool analytics, validator performance analysis, and long-term trend identification across the NEAR ecosystem.

## Key Use Cases
- Staking pool growth analysis and historical performance tracking
- Daily balance trend analysis and pattern identification
- Cross-pool performance comparison and benchmarking
- Staking pool health monitoring and risk assessment
- Long-term validator performance analysis and trend identification
- Staking adoption analysis and participation tracking
- Governance participation analysis and voting power distribution over time

## Important Relationships
- Aggregates data from `gov.fact_staking_pool_balances` for daily analysis
- Provides daily balance data for staking pool analytics and reporting
- Supports `gov.fact_staking_actions` with historical balance context
- Enables analysis in `stats.ez_core_metrics_hourly` for staking metrics
- Powers cross-pool analysis and long-term validator performance comparison
- Supports governance analytics and staking participation trend analysis

## Commonly-used Fields
- `date`: Essential for time-series analysis and trend detection
- `balance`: Critical for daily balance analysis and performance assessment
- `address`: Essential for staking pool identification and analysis
- `fact_staking_pool_daily_balances_id`: Important for unique record identification
- `inserted_timestamp`: Useful for data freshness and update tracking
- `modified_timestamp`: Important for data quality and update monitoring

{% enddocs %} 