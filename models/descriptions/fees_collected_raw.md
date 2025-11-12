{% docs fees_collected_raw %}

Raw JSON object containing fee information collected from the intent execution, as extracted from the DIP4 event log. This field contains the complete on-chain representation of fees charged by the intent protocol, formatted as a JSON object with token addresses as keys and unadjusted fee amounts as values. For example: `{"nep141:wrap.near": "1232145523809524"}` indicates fees collected in wrapped NEAR tokens. This field is null when no fees were collected or when fee information is not available in the event log. The raw format preserves the exact on-chain data structure for precise fee calculations and protocol revenue analysis.

{% enddocs %}
