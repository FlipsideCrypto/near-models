# staking fix

## methods 
### staking actions
*current*
 - deposit_and_stake
 - stake
 - unstake
 - unstake_all

 *investigate*
  - on_stake_action
    - has the most actions at 626k while deposit and stake is 2 at 400k. Could be the missing events
        - args are always empty, but this event could possible be used to find other staking events
  - liquid_unstake
  - on_staking_pool_withdraw
  - on_staking_pool_unstake
  - withdraw_all_from_staking_pool
  - on_get_account_unstaked_balance_to_withdraw_by_owner
  - distribute_staking
  - on_staking_pool_deposit_and_stake
  
### staking pools
*current*
 - create_staking_pool
 - update_reward_fee_fraction
 
 *investigate*
  - set_staking_pool(s)
  
*interesting*
  - refresh_staking_pool_balance
  - proxy_get_pool_info_callback
  - get_account_unstaked_balance
  - get_pool_info_callback

*unlikely, but lots of actions*
  - get_stable_pool
  - get_pool_shares
  - balance_stable_pool
  - get_pool

# solution
honing in on a solution by
 - looking for a `Stake` action in the events
 - receipts where a recipient is a staking pool
 - logs exist

## potential issues

### log_action not in ('unstaking', 'deposited')
 - 


### where not _log_signer_id_match
There are instances where the receipt signer does not match the address named in the log. 36.6k to be exact.
In once instance - the staker is `linear-protocol.near` while the signer is `operator.linear-protocol.near`. Others it is not so clear what the relationship is.
 - some (unk how many) might be addresses that are not yet defined `.near` addresses
 - seeing some other sub-addresses like `storage.herewallet.near` vs `owner.herewallet.near`
- i don't need to filter on this test so it's fine

### epoch rewards
There are also Epoch rewards in this pattern, which will make a good table on their own.
Logs are something like this:
> Epoch N: Contract received total rewards of X tokens. etc...
Example tx: '6UfgPwpwSxEb9WPvVh9jTHceTSK8U3wDLc7V8zQ6pGCu'
 - looks like this is what the discord user looked at to try and calculate total staked amount (close, was not exact)
 - query in snowflake/near/staking

 ### usn staking?
 usn-unofficial.poolowner.near
  - https://nearblocks.io/txns/5Zy7Vi8jCxiY6ErP7VvUUADM25yD3QawobGbnfaDxSTT#execution
  - https://nearblocks.io/txns/HbNHechdxs24HAf4Gk2P5ogq2gVnkE7S7hCmtynmRzid
  - exclude this pool as i dont think it is near staking
    - TODO - maybe name the table NEAR staking? or something like that

### aurora pool
aurora.pool.near
Seem to follow a pattern like 
> Record 0 TOKEN reward from farm #0"
 - is there an instance where the reward is not 0?
  - TODO - check where receiver_id is `aurora.pool.near` 
  - seeing this Record farm reward on `nativo.pool.near` as well
  - `stardust.pool.near`
=>    and logs[0] not ilike '%Record%reward from farm%'
