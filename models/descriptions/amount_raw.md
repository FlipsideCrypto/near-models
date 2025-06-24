{% docs amount_raw %}

Unadjusted amount of tokens as it appears on-chain (not decimal adjusted). This is the raw token amount before any decimal precision adjustments are applied. For example, if transferring 1 NEAR token, the amount_raw would be 1000000000000000000000000 (1e24) since NEAR has 24 decimal places. This field preserves the exact on-chain representation of the token amount for precise calculations and verification.

{% enddocs %}

{% docs amount_unadj %}

An unadjusted amount (of tokens, price, etc.) for the relevant record. This is the number as it appears on chain and is not decimal adjusted.

{% enddocs %}
