{% docs status %}

Boolean representing the success or failure of the event.
For transfers, both the tx and receipt must have succeeded for STATUS to be TRUE. 

NOTE - it is possible for a transaction to successfully execute while one of its receipts fails. A transaction can also be a failure with successful receipts. It is possible the approach taken to determine a successful transfer is incorrect, so both tx and receipt success fields are available.

{% enddocs %}
