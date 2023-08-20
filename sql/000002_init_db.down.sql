DROP TABLE IF EXISTS "bids_per_era" cascade;
DROP TABLE IF EXISTS "delegators_per_era" cascade;

DROP MATERIALIZED VIEW IF EXISTS "staked_amount_daily_candle_per_validator" cascade;
DROP MATERIALIZED VIEW IF EXISTS "rewards_era_cumulative_per_validator" cascade;
DROP MATERIALIZED VIEW IF EXISTS "rewards_daily_cumulative_per_validator" cascade;
DROP MATERIALIZED VIEW IF EXISTS "staking_operation_per_era_per_validator" cascade;
DROP MATERIALIZED VIEW IF EXISTS "staking_operation_daily_per_validator" cascade;
DROP MATERIALIZED VIEW IF EXISTS "delegators_daily_count_per_validator" cascade;
