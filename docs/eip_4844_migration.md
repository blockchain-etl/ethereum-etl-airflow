Run the following in BigQuery:

```sql
ALTER TABLE `your-project.your-dataset.blocks` ADD COLUMN IF NOT EXISTS blob_gas_used INT64 
    OPTIONS(description="The total amount of blob gas consumed by transactions in the block");
ALTER TABLE `your-project.your-dataset.blocks` ADD COLUMN IF NOT EXISTS excess_blob_gas INT64 
    OPTIONS(description="A running total of blob gas consumed in excess of the target, prior to the block. This is used to set blob gas pricing");

ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS max_fee_per_blob_gas INT64 
    OPTIONS(description="The maximum fee a user is willing to pay per blob gas");
ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS blob_versioned_hashes STRING REPEATED
    OPTIONS(description="A list of hashed outputs from kzg_to_versioned_hash");
ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS receipt_blob_gas_price INT64 
    OPTIONS(description="Blob gas price");
ALTER TABLE `your-project.your-dataset.transactions` ADD COLUMN IF NOT EXISTS receipt_blob_gas_used INT64 
    OPTIONS(description="Blob gas used");
```
