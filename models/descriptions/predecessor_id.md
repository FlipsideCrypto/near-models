{% docs predecessor_id %}

This is the account that was the immediate caller to the smart contract. If this is a simple transaction (no cross-contract calls) from alice.near to banana.near, the smart contract at banana.near considers Alice the predecessor. In this case, Alice would also be the signer.  
  
Source: https://docs.near.org/tutorials/crosswords/beginner/actions#predecessor-signer-and-current-account

{% enddocs %}
