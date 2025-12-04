{% docs intents__fact_bridges %}

## Description

The `intents__fact_bridges` table tracks cross-chain bridge transactions executed through the NEAR Protocol intents system (intents.near contract). This table captures token deposits and withdrawals facilitated by the Omnib ridge Multi-Token (OMFT) protocol using NEP245 multi-token transfer standard. Bridge transactions are identified by `mt_mint` (inbound deposits from other chains) and `mt_burn` (outbound withdrawals to other chains) events with deposit/withdraw memos.

Data is sourced from `intents__fact_transactions` and parses the token_id field to extract blockchain and contract address information. The table automatically determines bridge direction (inbound/outbound) and maps source/destination chains based on the log event type and blockchain identifier embedded in the token_id.

## Key Use Cases

- **Cross-Chain Bridge Analytics**: Track bridge volume, frequency, and patterns for multi-chain asset movement
- **Blockchain Flow Analysis**: Analyze which blockchains have the most inbound/outbound bridge activity
- **Token Bridge Monitoring**: Monitor specific token contracts being bridged to/from NEAR
- **Bridge Health Metrics**: Calculate success rates and identify failed bridge transactions
- **Platform Comparison**: Compare bridge activity across different bridge platforms and protocols

## Important Relationships

This table is part of the intents schema and relates to:
- **intents.fact_transactions**: Source table providing base intent transaction data with NEP245 mt_mint/mt_burn events
- **intents.ez_transactions**: Enriched view of intent transactions with token metadata and pricing
- **defi.fact_bridge_activity**: Main bridge fact table aggregating all bridge sources including intents

## Commonly-used Fields

- **direction**: Inbound (deposit from other chain to NEAR) or outbound (withdraw from NEAR to other chain)
- **source_chain / destination_chain**: Blockchain identifiers extracted from token_id (e.g., 'ethereum', 'polygon', 'near')
- **token_address**: Parsed contract address from the token_id
- **token_address_raw**: Full token_id with protocol prefix (e.g., 'nep141:ethereum-0xabc...omft.near')
- **amount_unadj / amount_adj**: Unadjusted and adjusted token amounts (currently identical, placeholder for future decimal conversion)
- **platform**: Always 'intents' for this table
- **bridge_address**: The intents.near contract address
- **receipt_succeeded**: Boolean indicating if the bridge transaction completed successfully

{% enddocs %}
