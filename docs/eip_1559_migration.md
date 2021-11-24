Run the following in BigQuery:

```sql
ALTER TABLE `your-project.your-dataset.blocks` ADD COLUMN IF NOT EXISTS base_fee_per_gas INT64 
    OPTIONS(description="Protocol base fee per gas, which can move up or down");

ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS max_fee_per_gas INT64 
    OPTIONS(description="Total fee that covers both base and priority fees");
ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS max_priority_fee_per_gas INT64 
    OPTIONS(description="Fee given to miners to incentivize them to include the transaction");
ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS transaction_type INT64 
    OPTIONS(description="Transaction type");
ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS receipt_effective_gas_price INT64 
    OPTIONS(description="The actual value per gas deducted from the senders account. Replacement of gas_price after EIP-1559");
```

To delete the new columns:

```sql
ALTER TABLE `your-project.your-dataset.blocks` DROP COLUMN base_fee_per_gas;

ALTER TABLE `your-project.your-dataset.transactions` DROP COLUMN max_fee_per_gas;
ALTER TABLE `your-project.your-dataset.transactions` DROP COLUMN max_priority_fee_per_gas;
ALTER TABLE `your-project.your-dataset.transactions` DROP COLUMN transaction_type;
ALTER TABLE `your-project.your-dataset.transactions` DROP COLUMN receipt_effective_gas_price;
```

EIP-1559 notes:

- London Mainnet Announcement: https://blog.ethereum.org/2021/07/15/london-mainnet-announcement/
- London upgrade will go live on mainnet on block **12965000**
- Legacy transactions are converted to EIP-1559 compatible transactions by using the legacy `gas_price` in the
    transaction as both `max_priority_fee_per_gas` and `max_fee_per_gas`.