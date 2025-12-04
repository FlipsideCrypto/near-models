{% docs fee_contract_address %}

Contract address of the fee token on its source blockchain. For NEAR native tokens, this is the NEAR contract address (e.g., "wrap.near"). For bridged tokens, this is the original contract address on the source chain (e.g., "0xdac17f958d2ee523a2206206994597c13d831ec7" for USDT on Ethereum). The value "native" indicates the blockchain's native token. This field enables precise token identification for pricing, metadata lookups, and cross-chain token tracking.

{% enddocs %}
