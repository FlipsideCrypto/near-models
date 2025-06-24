{% docs action_data %}

JSON object containing argument data for the receipt action. This field stores the parameters and arguments passed to the action, particularly for FunctionCall actions. The data structure varies by action type - for FunctionCall actions, it may include method names, arguments, and attached deposits. For Transfer actions, it contains the amount being transferred. This field enables detailed analysis of what specific operations were performed within each action.

{% enddocs %}
