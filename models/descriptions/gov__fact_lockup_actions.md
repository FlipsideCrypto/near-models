{% docs gov__fact_lockup_actions %}

## Description
This table contains all lockup account transactions on the NEAR Protocol blockchain, capturing lockup creation, vesting schedules, and release mechanisms. The data includes lockup amounts, duration periods, vesting schedules, and associated metadata, providing comprehensive tracking of token lockups and vesting arrangements. This table provides the foundation for lockup analytics, vesting schedule analysis, and token release tracking across the NEAR ecosystem.

## Key Use Cases
- Lockup account analysis and vesting schedule tracking
- Token release analysis and unlock pattern identification
- Vesting schedule compliance monitoring and analysis
- Lockup duration analysis and trend identification
- Cross-account lockup comparison and performance analysis
- Token distribution analysis and supply impact assessment
- Governance token lockup analysis and voting power tracking

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides lockup data for governance analytics and vesting analysis
- Supports `core.ez_native_daily_balances` with lockup balance tracking
- Enables analysis in `stats.ez_core_metrics_hourly` for lockup metrics
- Powers supply analysis and token distribution studies

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `lockup_account_id` and `owner_account_id`: Critical for account identification and ownership tracking
- `deposit`: Important for lockup amount analysis and value tracking
- `lockup_duration` and `release_duration`: Essential for vesting schedule analysis
- `lockup_timestamp` and `lockup_timestamp_ntz`: Important for timeline analysis and unlock tracking
- `vesting_schedule`: Critical for vesting pattern analysis and compliance monitoring

{% enddocs %} 