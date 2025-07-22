{% docs core__ez_native_daily_balances %}

## Description
This table provides daily balance snapshots for all accounts on the NEAR Protocol blockchain, capturing liquid balances, staking positions, lockup accounts, and storage usage. The data includes both regular accounts and lockup accounts (used for vesting schedules and staking), providing comprehensive balance tracking across different account types and balance categories. This table enables historical balance analysis, staking behavior tracking, and economic analysis of the NEAR ecosystem.

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