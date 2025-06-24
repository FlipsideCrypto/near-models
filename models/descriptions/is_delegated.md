{% docs is_delegated %}

Whether the action was delegated, typically a FunctionCall acting on another account on its behalf, often where receipt predecessor_id is not the receipt signer or receiver. This field indicates when an action is being executed by one account on behalf of another, which is common in relay transactions, cross-contract calls, or when using access keys with limited permissions. Note: Actions with action_name = 'Delegate' themselves are marked as FALSE, while the actions within them are marked as TRUE.

{% enddocs %}
