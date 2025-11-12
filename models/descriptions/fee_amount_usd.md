{% docs fee_amount_usd %}

USD value of the fee collected from the intent execution. This field provides the dollar equivalent value of the fee by multiplying the decimal-adjusted fee amount by the fee token's USD price at the time of the intent execution. This standardized USD representation enables protocol revenue analysis, fee tracking across different tokens, and cost comparisons over time. The field uses ZEROIFNULL to ensure zero values when price data is unavailable rather than null, which simplifies aggregations and revenue calculations. This field may be zero when price data is unavailable for new tokens, tokens with low liquidity, or during periods when price feeds are unavailable.

{% enddocs %}
