{% docs predecessor_id %}

Account that called the relevant receipt (often the same as tx_signer, but can be system as well). This field identifies the account that directly invoked the receipt being processed. In simple transactions, this is typically the same as the transaction signer. However, in cross-contract calls or system operations, this may be a different account or the system account. This is crucial for understanding the call chain and access control in NEAR's execution model.

{% enddocs %}
