{% docs tx_receiver %}

The transaction receiver, similar to an origin_to in EVM, but can be a contract OR if a relayer is used, the receiver is the user. This field identifies the account that is the intended recipient of the transaction. In direct transactions, this is typically the contract or account being called. In relay transactions, this represents the end user account that the relayer is acting on behalf of.

{% enddocs %}
