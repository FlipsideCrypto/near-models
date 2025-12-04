{% docs fee_decimals %}

Number of decimal places for the fee token. This field specifies the precision needed to convert from the raw on-chain fee amount to a human-readable decimal value. For example, if fee_decimals is 6, divide fee_amount_raw by 10^6 (1,000,000) to get the decimal-adjusted amount. Different tokens use different decimal precision based on their design (NEAR uses 24, USDT uses 6, most ERC-20s use 18).

{% enddocs %}
