{% docs token_price %}

Price of the token at the time of transfer (used for amount_usd column). This field contains the USD price per token unit at the time the transfer occurred. This price is used to calculate the amount_usd field by multiplying it with the decimal-adjusted token amount. Price data is sourced from various price feeds and may be null for tokens without available pricing information.

{% enddocs %} 