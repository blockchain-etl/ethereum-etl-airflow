Run the following in BigQuery:

```sql
ALTER TABLE `your-project.your-dataset.blocks` ADD COLUMN IF NOT EXISTS withdrawals_root STRING
    OPTIONS(description="The root of the withdrawal trie of the block");
ALTER TABLE `your-project.your-dataset.blocks` ADD COLUMN IF NOT EXISTS withdrawals ARRAY<STRUCT<index INT64, validator_index INT64, address STRING, amount STRING>>
    OPTIONS(description="Validator withdrawals");
```

To delete the new columns:

```sql
ALTER TABLE `your-project.your-dataset.blocks` DROP COLUMN withdrawals_root;
ALTER TABLE `your-project.your-dataset.blocks` DROP COLUMN withdrawals;
```

EIP-4895 notes:

- Shapella Mainnet Announcement: https://blog.ethereum.org/2023/03/28/shapella-mainnet-announcement
- Shapella upgrade will go live on mainnet at epoch **194048**
