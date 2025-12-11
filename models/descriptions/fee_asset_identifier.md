{% docs fee_asset_identifier %}

Extracted asset identifier from the fee token ID, containing only the contract address portion after the token standard prefix. This field is derived from the fee_token_id by extracting the contract address following the "nep141:", "nep171:", or "nep245:" prefix using regex pattern matching. For example, "nep141:wrap.near" becomes "wrap.near". This normalized format is used to join with token metadata tables and enable consistent token identification across different token standards.

{% enddocs %}
