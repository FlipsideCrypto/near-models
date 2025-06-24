{% docs signer_id %}

Signer of the receipt, often same as tx_signer, sometimes system in the case of systemic gas refunds. This field identifies the account that has the authority to execute the receipt. In most cases, this is the same as the transaction signer, but in system operations like gas refunds, this may be the system account. The signer is the account that originally signed the transaction that began the blockchain activity, which may or may not include cross-contract calls.

{% enddocs %}