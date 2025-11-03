{% docs core__ez_native_daily_balances %}

## Description
This table records daily balances for NEAR Accounts, according to the parameters set by the Near team's balances indexer. More details around the source data can be found by reading about the NEAR Public Lakehouse on their documentation site: https://docs.near.org/tools/indexing.
  
Please Note - Flipside does not index or curate the data in this table, we simply provide it as-is from the NEAR Public Lakehouse with a daily sync from the public access table located at `bigquery-public-data.crypto_near_mainnet_us.ft_balances_daily`.

## Key Use Cases
- Historical balance analysis and account growth tracking
- Staking behavior analysis and validator participation monitoring
- Lockup account analysis and vesting schedule tracking
- Storage usage analysis and network resource consumption
- Economic analysis of liquid vs staked token distribution
- Account activity correlation with balance changes
- Compliance and audit trail analysis for account balances

## Important Relationships
- Provides balance context for `core.fact_token_transfers` and `core.ez_token_transfers`
- Supports `gov.ez_staking_actions` with balance impact analysis
- Enables `gov.ez_lockup_actions` with lockup balance tracking
- Powers `stats.ez_core_metrics_hourly` with balance aggregation metrics
- Supports `atlas.ez_supply` with supply distribution analysis
- Provides foundation for account-level analytics and reporting

## Commonly-used Fields
- `account_id`: Essential for account-specific analysis and balance tracking
- `epoch_date`: Primary field for time-series analysis and balance history
- `liquid`: Critical for available balance analysis and spending patterns
- `staked`: Important for staking participation and validator analysis
- `lockup_liquid` and `lockup_staked`: Essential for lockup account analysis
- `storage_usage`: Useful for network resource consumption analysis
- `reward`: Important for staking reward analysis and economic incentives

{% enddocs %} 