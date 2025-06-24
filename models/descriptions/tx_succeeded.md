{% docs tx_succeeded %}

Boolean indicating if the transaction was successful (rarely used, most tx succeed, it is receipts that can fail). This field tracks the overall success status of the transaction. While most transactions succeed at the transaction level, individual receipts within the transaction can still fail. This field is less commonly used for filtering since receipt-level success tracking provides more granular information about specific operations within the transaction.

{% enddocs %}
