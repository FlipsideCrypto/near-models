{% docs intents__fact_swaps %}

## Description

The `intents__fact_swaps` table tracks swap transactions executed through the NEAR Protocol intents system (intents.near contract). This table captures token exchanges facilitated by the DIP4 (Decentralized Intent Protocol 4) standard, which enables intent-based trading where users specify desired outcomes rather than explicit execution paths. The table aggregates token_diff events from intent executions to identify swap pairs (token in/token out) and their corresponding amounts.

Data is sourced from `intents__fact_transactions` and enriched with action and log data to provide complete context for each swap. The table applies sophisticated logic to identify swap transactions from multi-token transfer events, determining input and output tokens based on balance changes recorded in DIP4 token_diff events.

## Key Use Cases

- **Intent-Based Swap Analytics**: Track swap volume, frequency, and patterns for intent-driven trading on NEAR
- **Token Pair Analysis**: Analyze which token pairs are most commonly swapped through the intents protocol
- **User Behavior Analysis**: Understand how users interact with intent-based swap execution
- **Referral Tracking**: Monitor referral activity and attribution for intent-based swaps
- **Protocol Metrics**: Calculate swap success rates, gas costs, and execution patterns for intent transactions

## Important Relationships

This table is part of the intents schema and relates to:
- **intents.fact_transactions**: Source table providing base intent transaction data with NEP245 and DIP4 events
- **intents.ez_transactions**: Enriched view of intent transactions with token metadata and pricing
- **core.ez_actions**: Provides transaction-level context including signer and action details
- **silver.logs_s3**: Source of raw log data for parsing DIP4 token_diff events

## Commonly-used Fields

- **intent_hash**: Unique identifier for the intent execution, used to track related transactions
- **token_in / token_out**: Token identifiers (with nep141/nep245 prefixes) showing what was received/sent
- **amount_in_raw / amount_out_raw**: Unadjusted token amounts (before decimal conversion) for in/out tokens
- **account_id**: The account that initiated or owns the intent execution
- **referral**: Optional referral account that facilitated the swap
- **swap_index**: Index for multiple swaps within a single transaction
- **swap_input_data**: JSON object containing detailed swap execution parameters and token_diff data

{% enddocs %}
