{% docs staking_action %}

The staking action performed in this transaction. Can be `"Stake"` or `"Unstake"` if in the deprecating `dim_staking_actions` table, or `staking`, `unstaking`, `deposited`, `withdrawing` if in the new `fact_staking_actions` table. These method names are taken directly from the log in which they occur.

{% enddocs %}
