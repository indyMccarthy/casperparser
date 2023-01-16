DROP TABLE IF EXISTS "blocks" cascade;
DROP TABLE IF EXISTS "raw_blocks" cascade;
DROP TABLE IF EXISTS "deploys" cascade;
DROP TABLE IF EXISTS "raw_deploys" cascade;
DROP TABLE IF EXISTS "contract_packages" cascade;
DROP TABLE IF EXISTS "contracts" cascade;
DROP TABLE IF EXISTS "named_keys" cascade;
DROP TABLE IF EXISTS "contracts_named_keys" cascade;
DROP TABLE IF EXISTS "rewards" cascade;
DROP TABLE IF EXISTS "bids" cascade;
DROP TABLE IF EXISTS "delegators" cascade;
DROP TABLE IF EXISTS "accounts" cascade;
DROP TABLE IF EXISTS "purses" cascade;
DROP VIEW IF EXISTS "full_stats" cascade;
DROP VIEW IF EXISTS "simple_stats" cascade;
DROP VIEW IF EXISTS "total_rewards" cascade;
DROP VIEW IF EXISTS "stakers" cascade;
DROP VIEW IF EXISTS "mouvements" cascade;
DROP VIEW IF EXISTS "rich_list" cascade;
DROP VIEW IF EXISTS "allowance" cascade;
DROP VIEW IF EXISTS "contracts_list" cascade;
DROP FUNCTION IF EXISTS "era_rewards" cascade;
DROP FUNCTION IF EXISTS "total_validator_rewards" cascade;
DROP FUNCTION IF EXISTS "total_account_rewards" cascade;
DROP FUNCTION IF EXISTS "block_details" cascade;
DROP FUNCTION IF EXISTS "contract_details" cascade;
DROP FUNCTION IF EXISTS "account_ercs20" cascade;
DROP FUNCTION IF EXISTS "erc20_holders" cascade;
DROP OWNED BY web_anon;
DROP ROLE IF EXISTS web_anon;