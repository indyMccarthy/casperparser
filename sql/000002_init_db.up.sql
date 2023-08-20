

CREATE TABLE "bids_per_era"
(
    "block_height"              BIGINT      NOT NULL,
    "validator_public_key"      VARCHAR(68) NOT NULL,
    "bonding_purse"             VARCHAR     NOT NULL,
    "staked_amount"             NUMERIC     NOT NULL,
    "delegation_rate"           INT         NOT NULL,
    "inactive"                  BOOL        NOT NULL,
    PRIMARY KEY("block_height", "validator_public_key", "bonding_purse")
);

CREATE INDEX ON "bids_per_era" ("validator_public_key");
CREATE INDEX ON "bids_per_era" ("block_height");

CREATE TABLE "delegators_per_era"
(
    "block_height"          BIGINT      NOT NULL,
    "delegator_public_key"  VARCHAR(68) NOT NULL,
    "validator_public_key"  VARCHAR(68) NOT NULL,
    "staked_amount"         NUMERIC     NOT NULL,
    "bonding_purse"         VARCHAR     NOT NULL,
    PRIMARY KEY("block_height", "validator_public_key", "delegator_public_key")
);

CREATE INDEX ON "delegators_per_era" ("validator_public_key");
CREATE INDEX ON "delegators_per_era" ("block_height");


--- --- --- --- --- IMPORTANT NOTE --- --- --- --- ---
-- The following views are materialized views.
-- It is strongly recommended to not use them as is on a 
-- standard database.
-- We use incremental refresh for the materialized views 
-- and streaming databases to use the following views.
-- In a standard database, these materialized views would not be
-- incrementaly refreshed and could be extremely costly due to the 
-- volume of processed data, nested operations, CTEs, etc.
--- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- Staked amount per validator
CREATE MATERIALIZED VIEW IF NOT EXISTS "staked_amount_daily_candle_per_validator" AS
SELECT 
    block_joined_era_info.date,
    block_joined_era_info.validator_public_key,
    block_joined_era_info.open_staked_amount,
    avg(block_joined_era_info.staked_amount) AS avg_staked_amount,
    max(block_joined_era_info.staked_amount) AS max_staked_amount,
    min(block_joined_era_info.staked_amount) AS min_staked_amount,
    lead(block_joined_era_info.open_staked_amount) OVER (PARTITION BY block_joined_era_info.validator_public_key ORDER BY block_joined_era_info.date) AS close_staked_amount
FROM (
    SELECT 
        date(blocks."timestamp") AS date,
        stacked_amount_info.validator_public_key,
        stacked_amount_info.staked_amount,
        first_value(stacked_amount_info.staked_amount) OVER (PARTITION BY stacked_amount_info.validator_public_key, (date(blocks."timestamp")) ORDER BY blocks."timestamp") AS open_staked_amount
    FROM (
        SELECT 
            unioned_staked_amount.block_height,
            unioned_staked_amount.validator_public_key,
            sum(unioned_staked_amount.staked_amount) AS staked_amount
        FROM ( 
            SELECT 
                delegators_per_era.block_height,
                delegators_per_era.validator_public_key,
                sum(delegators_per_era.staked_amount) AS staked_amount
            FROM delegators_per_era
            GROUP BY 
                delegators_per_era.block_height,
                delegators_per_era.validator_public_key
            UNION ALL
            SELECT 
                bids_per_era.block_height,
                bids_per_era.validator_public_key,
                max(bids_per_era.staked_amount) AS staked_amount
            FROM bids_per_era
            GROUP BY 
                bids_per_era.block_height,
                bids_per_era.validator_public_key
        ) unioned_staked_amount
        GROUP BY 
            unioned_staked_amount.block_height,
            unioned_staked_amount.validator_public_key
    ) stacked_amount_info
    LEFT JOIN blocks 
    ON blocks.height = stacked_amount_info.block_height
) block_joined_era_info
GROUP BY 
    block_joined_era_info.date,
    block_joined_era_info.validator_public_key,
    block_joined_era_info.open_staked_amount
WITH DATA;

-- Rewards per validator
CREATE MATERIALIZED VIEW IF NOT EXISTS "rewards_era_cumulative_per_validator" AS
WITH tmp_rewards_validator AS (
    SELECT 
        rewards.era,
        rewards.validator_public_key,
        sum(rewards.amount::numeric) AS amount
    FROM rewards
    WHERE rewards.delegator_public_key IS NULL
    GROUP BY 
        rewards.era, 
        rewards.validator_public_key
), 
        
tmp_rewards_delegator AS (
    SELECT 
        rewards.era,
        rewards.validator_public_key,
        sum(rewards.amount::numeric) AS amount
    FROM rewards
    WHERE rewards.delegator_public_key IS NOT NULL
    GROUP BY 
        rewards.era, 
        rewards.validator_public_key
)

SELECT 
    rew.era,
    rew.validator_public_key,
    vrew.era_cumulative_validator_rewards,
    rew.era_cumulative_delegators_rewards
FROM ( 
    SELECT 
        tmp_rewards_delegator.era,
        tmp_rewards_delegator.validator_public_key,
        sum(tmp_rewards_delegator.amount) OVER (PARTITION BY tmp_rewards_delegator.validator_public_key ORDER BY tmp_rewards_delegator.era) AS era_cumulative_delegators_rewards
    FROM tmp_rewards_delegator
) rew
LEFT JOIN ( 
    SELECT 
        tmp_rewards_validator.era,
        tmp_rewards_validator.validator_public_key,
        sum(tmp_rewards_validator.amount) OVER (PARTITION BY tmp_rewards_validator.validator_public_key ORDER BY tmp_rewards_validator.era) AS era_cumulative_validator_rewards
    FROM tmp_rewards_validator
) vrew 
ON vrew.validator_public_key::text = rew.validator_public_key::text 
AND vrew.era = rew.era
WITH DATA;


CREATE MATERIALIZED VIEW IF NOT EXISTS "rewards_daily_cumulative_per_validator" AS
SELECT 
    bl.date_era_end,
    per_era.validator_public_key,
    max(per_era.era_cumulative_validator_rewards) AS daily_cumulative_validator_rewards,
    max(per_era.era_cumulative_delegators_rewards) AS daily_cumulative_delegators_rewards
FROM (
    SELECT 
        era,
        validator_public_key,
        sum(era_cumulative_validator_rewards) OVER (PARTITION BY validator_public_key ORDER BY era) AS era_cumulative_delegators_rewards,
        sum(era_cumulative_delegators_rewards) OVER (PARTITION BY validator_public_key ORDER BY era) AS era_cumulative_validator_rewards
    FROM rewards_era_cumulative_per_validator
) per_era
LEFT JOIN (
    SELECT 
        era,
        date(max("timestamp")) AS date_era_end
    FROM blocks
    GROUP BY era
) bl ON per_era.era = bl.era
GROUP BY 
    bl.date_era_end, 
    per_era.validator_public_key
WITH DATA;

-- Staking operations per validator
CREATE MATERIALIZED VIEW IF NOT EXISTS "staking_operation_per_era_per_validator" AS
SELECT 
    rich_blocks.era,
    rich_blocks.date,
    stakingop.validator_public_key,
    stakingop.staking_operation,
    count(1) AS count_op,
    sum(stakingop.amount) / 1000000000.0 AS amount_approx
FROM ( 
    SELECT 
        deploys.block,
        deploys.metadata_type,
        deploys.metadata,
        COALESCE(
            CASE
                WHEN deploys.metadata_type::text = 'undelegate'::text OR (deploys.metadata ->> 'action'::text) = 'undelegate'::text OR deploys.metadata_type::text = 'leave_staking'::text THEN ((deploys.metadata ->> 'amount'::text)::numeric) * '-1'::integer::numeric
                ELSE (deploys.metadata ->> 'amount'::text)::numeric
            END, 
            0::numeric
        ) AS amount,
        CASE
            WHEN deploys.metadata_type::text = 'undelegate'::text OR (deploys.metadata ->> 'action'::text) = 'undelegate'::text OR deploys.metadata_type::text = 'leave_staking'::text THEN 'undelegate'::text
            ELSE 'delegate'::text
        END AS staking_operation,
        (deploys.metadata ->> 'validator'::text)::character varying AS validator_public_key
    FROM deploys
    WHERE 
        deploys.result::text = 'success'::text 
        AND (deploys.metadata_type::text = 'delegate'::text OR deploys.metadata_type::text = 'undelegate'::text OR deploys.metadata_type::text = 'leave_staking'::text OR deploys.metadata_type::text = 'enter_staking'::text OR deploys.metadata_type::text = 'moduleBytes'::text AND ((deploys.metadata ->> 'action'::text) = 'undelegate'::text OR (deploys.metadata ->> 'action'::text) = 'delegate'::text))
) stakingop
LEFT JOIN (
    SELECT 
        blocks.hash,
        date(blocks."timestamp") AS date,
        blocks.era
    FROM blocks
) rich_blocks 
ON rich_blocks.hash::text = stakingop.block::text
GROUP BY 
    rich_blocks.era, 
    rich_blocks.date, 
    stakingop.validator_public_key, 
    stakingop.staking_operation
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS "staking_operation_daily_per_validator" AS
SELECT 
    date,
    validator_public_key,
    staking_operation,
    sum(count_op) AS count_op,
    sum(amount_approx) AS amount_approx
FROM staking_operation_per_era_per_validator
GROUP BY 
    date,
    validator_public_key,
    staking_operation
WITH DATA;

CREATE INDEX ON "staking_operation_per_era_per_validator" USING btree;
CREATE INDEX ON "staking_operation_daily_per_validator" USING btree;

-- Delegators per validator
CREATE MATERIALIZED VIEW IF NOT EXISTS "delegators_daily_count_per_validator" AS
SELECT 
    date(blocks."timestamp") AS date,
    gb_blk.validator_public_key,
    avg(gb_blk.delegator_count) AS avg
FROM ( 
    SELECT 
        dpe.block_height,
        dpe.validator_public_key,
        count(DISTINCT dpe.delegator_public_key) AS delegator_count
    FROM delegators_per_era dpe
    GROUP BY 
        dpe.block_height,
        dpe.validator_public_key
) gb_blk
LEFT JOIN blocks
ON blocks.height = gb_blk.block_height
GROUP BY 
    (date(blocks."timestamp")),
    gb_blk.validator_public_key
WITH DATA;
