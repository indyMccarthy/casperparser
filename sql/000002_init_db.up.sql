

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

