{% docs fee_token %}

Token symbol for the fee collected (e.g., 'NEAR', 'USDT'). This field identifies the specific token in which the intent execution fee was charged, extracted and labeled from the fees_collected_raw JSON object. Fee tokens are typically stablecoins (like USDT, USDC) or the native protocol token (NEAR), though any supported token can be used for fees. This field is null when no fees were collected or when the fee token cannot be identified in the token metadata. Understanding fee tokens is important for protocol revenue analysis and cost tracking across different intent executions.

{% enddocs %}
