{% docs receipt_succeeded %}

Boolean indicating whether the receipt was successfully processed. This field tracks the execution status of the receipt, which is crucial for understanding transaction outcomes. While most transactions succeed, individual receipts within a transaction can fail due to various reasons such as insufficient gas, contract errors, or invalid parameters. This field is essential for filtering successful operations and analyzing failure patterns.

{% enddocs %}