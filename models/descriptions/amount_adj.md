{% docs amount_adj %}

Decimal adjusted amount of tokens (as float, rounded - use this generally). This field provides the token amount after applying the appropriate decimal precision adjustments based on the token's decimal places. For example, if transferring 1 NEAR token, the amount_adj would be 1.0 after dividing the raw amount (1e24) by 10^24. This field is the most commonly used representation for token amounts in analytics and reporting as it provides human-readable values.

{% enddocs %}
