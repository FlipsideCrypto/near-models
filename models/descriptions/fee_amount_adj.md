{% docs fee_amount_adj %}

Decimal-adjusted fee amount collected from the intent execution. This field provides the fee amount after applying the appropriate decimal precision adjustments based on the fee token's decimal places. For example, if a fee of 1 USDT was collected, the fee_amount_adj would be 1.0 after dividing the raw amount by 10^6 (USDT has 6 decimal places). This field is the most commonly used representation for fee amounts in analytics and reporting as it provides human-readable values. This field is null when no fees were collected or when the fee token's decimal information is unavailable.

{% enddocs %}
