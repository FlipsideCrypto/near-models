{% docs fee_amount_raw %}

Unadjusted raw fee amount as it appears on-chain before decimal adjustments. This field contains the exact fee amount value from the fees_collected object in the DIP4 log, represented as a string to preserve precision for large numbers. To convert to decimal-adjusted amounts, divide by 10^decimals using the fee_decimals field. This raw format is essential for precise fee calculations, reconciliation with on-chain data, and maintaining numerical accuracy for high-value transactions.

{% enddocs %}
