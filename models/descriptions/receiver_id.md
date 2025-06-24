{% docs receiver_id %}

Account reacting to the receipt from predecessor_id, can be relay, a contract, or a user of a relay, etc. This field identifies the account that is processing the receipt and executing the associated action. In most cases, this is a smart contract that is being called, but it can also be a user account in relay transactions or the system account for certain operations. This differs from tx_receiver and is specific to the receipt being processed.

{% enddocs %}
