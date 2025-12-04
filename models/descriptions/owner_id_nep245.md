{% docs owner_id_nep245 %}

Account ID that owns the multi-token (NEP245) assets involved in the intent transaction. For mint events, this is the account receiving the newly created tokens. For burn events, this is the account whose tokens are being destroyed. This field is null for transfer events, which use old_owner_id and new_owner_id instead. The owner_id is critical for tracking token custody and account balances in multi-token intent executions.

{% enddocs %}
