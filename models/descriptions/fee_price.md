{% docs fee_price %}

USD price of the fee token at the time of the intent execution, used to calculate fee_amount_usd. This price is derived from Flipside's crosschain price feeds using ASOF JOIN logic to get the most recent price at or before the block_timestamp. For stablecoin fees (symbols matching 'USD%'), the price defaults to 1.0 if no price data is available. The price matching prioritizes contract address matches over symbol matches to ensure accuracy for tokens with similar symbols.

{% enddocs %}
