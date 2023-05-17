{% docs gas_burnt %}

The gas used in the object, whether it be the transaction, action or receipt. In the case of a curated model where this column is included alongside a transaction fee, this is the gas burnt during the individual action or receipt.

In raw number format with 16 decimal places, to adjust divide by POW(10,16) or multiply by 1e-16.

{% enddocs %}
