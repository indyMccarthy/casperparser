// Package tasks define block task payload & handler
package tasks

import (
	"casperParser/db"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
)

// TypeBlockRaw Task block raw insert
// TypeBlockVerify Task block verify
const (
	TypeBlockRaw       = "block:raw"
	TypeBlockVerify    = "block:verify"
	TypeNativeTransfer = "native_transfer"
)

// NewBlockRawTask used for not yet parsed blocks
func NewBlockRawTask(blockHeight int) (*asynq.Task, error) {
	payload, err := json.Marshal(BlockRawPayload{BlockHeight: blockHeight})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeBlockRaw, payload), nil
}

// NewBlockVerifyTask used to verify blocks
func NewBlockVerifyTask(blockHash string) (*asynq.Task, error) {
	payload, err := json.Marshal(BlockVerifyPayload{BlockHash: blockHash})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeBlockVerify, payload), nil
}

// HandleBlockRawTask retrieve and parse a certain block height, insert it in the database, and add all deploys included in the blocks to the queue
func HandleBlockRawTask(ctx context.Context, t *asynq.Task) error {
	var p BlockRawPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}

	result, block, err := WorkerRpcClient.GetBlock(p.BlockHeight)
	if err != nil {
		return err
	}

	var database = db.DB{Postgres: WorkerPool}
	eraEnd := result.Block.Header.EraEnd != nil
	err = database.InsertBlock(ctx, result.Block.Hash, result.Block.Header.EraID, result.Block.Header.Timestamp, result.Block.Header.Height, eraEnd, string(block))
	if err != nil {
		return err
	}

	if eraEnd {
		addEraToQueue(result.Block.Hash)
		addAuctionEraToQueue(result.Block.Header.Height)
	}

	for _, s := range result.Block.Body.TransferHashes {
		addDeployToQueue(s)
		// TODO: Deploy info does not contain timestamp, so we take Block timestamps which is not accurate, even false sometimes
		addDeployToDeployInfoQueue(s, result.Block.Hash, result.Block.Header.StateRootHash, result.Block.Header.Timestamp)
	}
	for _, s := range result.Block.Body.DeployHashes {
		addDeployToQueue(s)
		// TODO: Deploy info does not contain timestamp, so we take Block timestamps which is not accurate, even false sometimes
		addDeployToDeployInfoQueue(s, result.Block.Hash, result.Block.Header.StateRootHash, result.Block.Header.Timestamp)
	}
	return nil
}

// HandleBlockVerifyTask retrieve and verify that all deploys of a block are inserted in the db
func HandleBlockVerifyTask(ctx context.Context, t *asynq.Task) error {
	var p BlockVerifyPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}

	var database = db.DB{Postgres: WorkerPool}
	block, err := database.GetRawBlock(ctx, p.BlockHash)
	if err != nil {
		return err
	}
	allDeploys := append(block.Block.Body.DeployHashes, block.Block.Body.TransferHashes...)
	if len(allDeploys) == 0 {
		return database.ValidateBlock(ctx, p.BlockHash)
	}
	countDeploys, err := database.CountDeploys(ctx, allDeploys)
	if err != nil {
		return err
	}
	if countDeploys != len(allDeploys) {
		for _, s := range allDeploys {
			addDeployToQueue(s)
		}
	} else {
		return database.ValidateBlock(ctx, p.BlockHash)
	}
	return nil
}

func addAuctionEraToQueue(blockheight int) {
	task, err := NewAuctionEraTask(blockheight)
	if err != nil {
		log.Printf("could not create task: %v\n", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("auctionera"))
	if err != nil {
		log.Printf("could not enqueue task: %v\n", err)
	}
}

// addDeployToQueue a deploy hash to the queue
func addDeployToQueue(hash string) {
	task, err := NewDeployRawTask(hash)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("deploys"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

// addDeployToQueue a deploy hash to the queue
func addDeployToDeployInfoQueue(hash string, blockHash string, stateRootHash string, deployTimestamp string) {
	task, err := NewDeployInfoRawTask(hash, blockHash, stateRootHash, deployTimestamp)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("deployinfos"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

// addDeployToQueue a deploy hash to the queue
func addTransferToQueue(hash string, blockHash string, deployHash string, deployTimestamp string, stateRootHash string) {
	task, err := NewTransferRawTask(hash, blockHash, deployHash, deployTimestamp, stateRootHash)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("transfers"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

// addEraToQueue a era hash to the queue
func addEraToQueue(hash string) {
	task, err := NewRewardTask(hash)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("era"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

type BlockRawPayload struct {
	BlockHeight int
}

type BlockVerifyPayload struct {
	BlockHash string
}
