CREATE SCHEMA IF NOT EXISTS ethereum;

DROP TABLE IF EXISTS ethereum.blocks;

CREATE TABLE ethereum.blocks (
  number            BIGINT         NOT NULL,     -- The block number
  hash              VARCHAR(65535) NOT NULL,     -- Hash of the block
  parent_hash       VARCHAR(65535) NOT NULL,     -- Hash of the parent block
  nonce             VARCHAR(65535) NOT NULL,     -- Hash of the generated proof-of-work
  sha3_uncles       VARCHAR(65535) NOT NULL,     -- SHA3 of the uncles data in the block
  logs_bloom        VARCHAR(65535) NOT NULL,     -- The bloom filter for the logs of the block
  transactions_root VARCHAR(65535) NOT NULL,     -- The root of the transaction trie of the block
  state_root        VARCHAR(65535) NOT NULL,     -- The root of the final state trie of the block
  receipts_root     VARCHAR(65535) NOT NULL,     -- The root of the receipts trie of the block
  miner             VARCHAR(65535) NOT NULL,     -- The address of the beneficiary to whom the mining rewards were given
  difficulty        NUMERIC(38, 0) NOT NULL,     -- Integer of the difficulty for this block
  total_difficulty  NUMERIC(38, 0) NOT NULL,     -- Integer of the total difficulty of the chain until this block
  size              BIGINT         NOT NULL,     -- The size of this block in bytes
  extra_data        VARCHAR(65535) DEFAULT NULL, -- The extra data field of this block
  gas_limit         BIGINT         DEFAULT NULL, -- The maximum gas allowed in this block
  gas_used          BIGINT         DEFAULT NULL, -- The total used gas by all transactions in this block
  timestamp         BIGINT         NOT NULL,     -- The unix timestamp for when the block was collated
  transaction_count BIGINT         NOT NULL,     -- The number of transactions in the block
  PRIMARY KEY (number)
)
DISTKEY (number)
SORTKEY (timestamp);

--

DROP TABLE IF EXISTS ethereum.contracts;

CREATE TABLE ethereum.contracts (
  address            VARCHAR(65535) NOT NULL, -- Address of the contract
  bytecode           VARCHAR(65535) NOT NULL, -- Bytecode of the contract
  function_sighashes VARCHAR(65535) NOT NULL, -- 4-byte function signature hashes
  is_erc20           BOOLEAN        NOT NULL, -- Whether this contract is an ERC20 contract
  is_erc721          BOOLEAN        NOT NULL, -- Whether this contract is an ERC721 contract
  PRIMARY KEY (address)
)
DISTKEY (address)
SORTKEY (address);

--

DROP TABLE IF EXISTS ethereum.logs;

CREATE TABLE ethereum.logs (
  log_index         BIGINT         NOT NULL, -- Integer of the log index position in the block
  transaction_hash  VARCHAR(65535) NOT NULL, -- Hash of the transactions this log was created from
  transaction_index BIGINT         NOT NULL, -- Integer of the transactions index position log was created from
  block_hash        VARCHAR(65535) NOT NULL, -- Hash of the block where this log was in
  block_number      BIGINT         NOT NULL, -- The block number where this log was in
  address           VARCHAR(65535) NOT NULL, -- Address from which this log originated
  data              VARCHAR(65535) NOT NULL, -- Contains one or more 32 Bytes non-indexed arguments of the log
  topics            VARCHAR(65535) NOT NULL, -- Indexed log arguments (0 to 4 32-byte hex strings). (In solidity: The first topic is the hash of the signature of the event (e.g. Deposit(address,bytes32,uint256)), except you declared the event with the anonymous specifier
  PRIMARY KEY (block_number, log_index)
)
DISTKEY (block_number)
SORTKEY (log_index);

--

DROP TABLE IF EXISTS ethereum.receipts;

CREATE TABLE ethereum.receipts (
  transaction_hash    VARCHAR(65535) NOT NULL,     -- Hash of the transaction
  transaction_index   BIGINT         NOT NULL,     -- Integer of the transactions index position in the block
  block_hash          VARCHAR(65535) NOT NULL,     -- Hash of the block where this transaction was in
  block_number        BIGINT         NOT NULL,     -- Block number where this transaction was in
  cumulative_gas_used BIGINT         NOT NULL,     -- The total amount of gas used when this transaction was executed in the block
  gas_used            BIGINT         NOT NULL,     -- The amount of gas used by this specific transaction alone
  contract_address    VARCHAR(65535) DEFAULT NULL, -- The contract address created, if the transaction was a contract creation, otherwise null
  root                VARCHAR(65535) DEFAULT NULL, -- 32 bytes of post-transaction stateroot (pre Byzantium)
  status              BIGINT         DEFAULT NULL, -- Either 1 (success) or 0 (failure) (post Byzantium)
  PRIMARY KEY (transaction_hash)
)
DISTKEY (block_number)
SORTKEY (transaction_index);

--

DROP TABLE IF EXISTS ethereum.token_transfers;

CREATE TABLE ethereum.token_transfers (
  token_address    VARCHAR(65535) NOT NULL, -- ERC20 token address
  from_address     VARCHAR(65535) NOT NULL, -- Address of the sender
  to_address       VARCHAR(65535) NOT NULL, -- Address of the receiver
  value            VARCHAR(65535) NOT NULL, -- Amount of tokens transferred (ERC20) / id of the token transferred (ERC721). Cast to NUMERIC or FLOAT8
  transaction_hash VARCHAR(65535) NOT NULL, -- Transaction hash
  log_index        BIGINT         NOT NULL, -- Log index in the transaction receipt
  block_number     BIGINT         NOT NULL, -- The block number
  PRIMARY KEY (transaction_hash)
)
DISTKEY (block_number)
SORTKEY (log_index);

--

DROP TABLE IF EXISTS ethereum.tokens;

CREATE TABLE ethereum.tokens (
  address      VARCHAR(65535) NOT NULL,     -- The address of the ERC20 token
  symbol       VARCHAR(65535) DEFAULT NULL, -- The symbol of the ERC20 token
  name         VARCHAR(65535) DEFAULT NULL, -- The name of the ERC20 token
  decimals     VARCHAR(65535) DEFAULT NULL, -- The number of decimals the token uses.  Cast to NUMERIC or FLOAT8
  total_supply VARCHAR(65535) NOT NULL,     -- The total token supply. Cast to NUMERIC or FLOAT8
  PRIMARY KEY (address)
)
DISTKEY (address)
SORTKEY (address);

--

DROP TABLE IF EXISTS ethereum.traces;

CREATE TABLE ethereum.traces (
  block_number      BIGINT         NOT NULL, -- Block number where this trace was in
  transaction_hash  VARCHAR(65535) NOT NULL, -- Transaction hash where this trace was in
  transaction_index BIGINT         NOT NULL, -- Integer of the transactions index position in the block
  from_address      VARCHAR(65535) NOT NULL, -- Address of the sender, null when trace_type is genesis or reward
  to_address        VARCHAR(65535) NOT NULL, -- Address of the receiver if trace_type is call, address of new contract or null if trace_type is create, beneficiary address if trace_type is suicide, miner address if trace_type is reward, shareholder address if trace_type is genesis, WithdrawDAO address if trace_type is daofork
  value             NUMERIC(38, 0) NOT NULL, -- Value transferred in Wei
  input             VARCHAR(65535) NOT NULL, -- The data sent along with the message call
  output            VARCHAR(65535) NOT NULL, -- The output of the message call, bytecode of contract when trace_type is create
  trace_type        VARCHAR(65535) NOT NULL, -- One of call, create, suicide, reward, genesis, daofork
  call_type         VARCHAR(65535) NOT NULL, -- One of call, callcode, delegatecall, staticcall
  reward_type       VARCHAR(65535) NOT NULL, -- One of block, uncle
  gas               BIGINT         NOT NULL, -- Gas provided with the message call
  gas_used          BIGINT         NOT NULL, -- Gas used by the message call
  subtraces         BIGINT         NOT NULL, -- Number of subtraces
  trace_address     VARCHAR(65535) NOT NULL, -- Comma separated list of trace address in call tree
  error             VARCHAR(65535) NOT NULL  -- Error if message call failed
)
DISTKEY (block_number)
SORTKEY (transaction_index);

--

DROP TABLE IF EXISTS ethereum.transactions;

CREATE TABLE ethereum.transactions (
  hash              VARCHAR(65535) NOT NULL,     -- Hash of the transaction
  nonce             BIGINT         NOT NULL,     -- The number of transactions made by the sender prior to this one
  block_hash        VARCHAR(65535) NOT NULL,     -- Hash of the block where this transaction was in
  block_number      BIGINT         NOT NULL,     -- Block number where this transaction was in
  transaction_index BIGINT         NOT NULL,     -- Integer of the transactions index position in the block
  from_address      VARCHAR(65535) NOT NULL,     -- Address of the sender
  to_address        VARCHAR(65535) DEFAULT NULL, -- Address of the receiver. null when its a contract creation transaction
  value             NUMERIC(38, 0) NOT NULL,     -- Value transferred in Wei
  gas               BIGINT         NOT NULL,     -- Gas provided by the sender
  gas_price         BIGINT         NOT NULL,     -- Gas price provided by the sender in Wei
  input             VARCHAR(65535) NOT NULL,     -- The data sent along with the transaction
  PRIMARY KEY (hash)
)
DISTKEY (block_number)
SORTKEY (block_number);
