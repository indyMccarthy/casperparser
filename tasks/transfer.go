// Package tasks Define the transfer task payload and handler
package tasks

import (
	"casperParser/db"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hibiken/asynq"
)

// TypeTransferRaw Task transfer raw type
// TypeTransferKnown Task transfer known type
const (
	TypeTransferRaw   = "transfer:raw"
	TypeTransferKnown = "transfer:known"
)

// NewTransferRawTask Used for not yet parsed transfer
func NewTransferRawTask(hash string, blockHash string, deployHash string, deployTimestamp string, stateRootHash string) (*asynq.Task, error) {
	payload, err := json.Marshal(TransferRawPayload{TransferHash: hash, Block: blockHash, Deploy: deployHash, StateRootHash: stateRootHash})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeTransferRaw, payload), nil
}

// NewTransferKnownTask used for already parsed transfer
//func NewTransferKnownTask(hash string, blockHash string, deployHash string, stateRootHash string) (*asynq.Task, error) {
//	payload, err := json.Marshal(TransferKnownPayload{TransferHash: hash, Block: blockHash, Deploy: deployHash, StateRootHash: stateRootHash})
//	if err != nil {
//		return nil, err
//	}
//	return asynq.NewTask(TypeTransferKnown, payload), nil
//}

// HandleTransferRawTask fetch a transfer from the rpc endpoint, parse it, and insert it in the database
func HandleTransferRawTask(ctx context.Context, t *asynq.Task) error {
	var p TransferRawPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}

	rpcTransfer, resp, err := WorkerRpcClient.GetTransfer(p.StateRootHash, p.TransferHash)
	if err != nil {
		println("ERROR on WorkerRpcClient.GetTransfer(p.StateRootHash, p.TransferHash)")
		println(p.StateRootHash)
		println(p.TransferHash)
		return err
	}

	var database = db.DB{Postgres: WorkerPool}
	jsonString := strings.ReplaceAll(string(resp), "\\u0000", "")
	amount, err := strconv.Atoi(rpcTransfer.StoredValue.Transfer.Amount)
	if err != nil {
		return err
	}
	gas, err := strconv.Atoi(rpcTransfer.StoredValue.Transfer.Gas)
	if err != nil {
		return err
	}
	err = database.InsertTransfer(ctx, p.TransferHash, p.Block, p.Deploy, rpcTransfer.StoredValue.Transfer.From, rpcTransfer.StoredValue.Transfer.To, rpcTransfer.StoredValue.Transfer.Source, rpcTransfer.StoredValue.Transfer.Target, amount, gas, jsonString, fmt.Sprint(rpcTransfer.StoredValue.Transfer.Id))
	if err != nil {
		println("ERROR on InsertTransfer")
		//println(p.TransferHash, p.Block, p.Deploy, rpcTransfer.StoredValue.Transfer.From, rpcTransfer.StoredValue.Transfer.To, rpcTransfer.StoredValue.Transfer.Source, rpcTransfer.StoredValue.Transfer.Target, rpcTransfer.StoredValue.Transfer.Amount, rpcTransfer.StoredValue.Transfer.Gas, rpcTransfer.StoredValue.Transfer.Id)
		return err
	}

	prefix := "account-hash-"

	accountHashFromTask, err := NewAccountHashTask(strings.TrimPrefix(rpcTransfer.StoredValue.Transfer.From, prefix))
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(accountHashFromTask, asynq.Queue("accounts"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}

	accountHashToTask, err := NewAccountHashTask(strings.TrimPrefix(rpcTransfer.StoredValue.Transfer.To, prefix))
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(accountHashToTask, asynq.Queue("accounts"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}

	return nil
}

// HandleTransferKnownTask fetch a transfer from the database, parse it, and insert it in the database
func HandleTransferKnownTask(ctx context.Context, t *asynq.Task) error {
	//var p TransferKnownPayload
	//if err := json.Unmarshal(t.Payload(), &p); err != nil {
	//	return fmt.Errorf("json.Unmarshal failed: %v", err)
	//}
	//var database = db.DB{Postgres: WorkerPool}
	//dbTransfer, err := database.GetTransfer(ctx, p.TransferHash)
	//if err != nil {
	//	log.Printf("Can't find transfer %s\n", p.TransferHash)
	//	return err
	//}
	//
	//var database = db.DB{Postgres: WorkerPool}
	//err = database.UpdateTransfer(ctx, p.TransferHash, p.Block, p.Deploy, rpcTransfer.StoredValue.from, rpcTransfer.StoredValue.to, rpcTransfer.StoredValue.source, rpcTransfer.StoredValue.target, rpcTransfer.StoredValue.amount, rpcTransfer.StoredValue.gas, rpcTransfer.StoredValue.id)
	//if err != nil {
	//	return err
	//}

	return nil
}

// addAccountHashToQueue add a account hash to the queue
func addAccountHashToQueue(hash string) {
	task, err := NewAccountHashTask(hash)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("accounts"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

type TransferRawPayload struct {
	TransferHash    string
	Block           string
	Deploy          string
	StateRootHash   string
}

type TransferKnownPayload struct {
	TransferHash    string
	Block           string
	Deploy          string
	StateRootHash   string
}
