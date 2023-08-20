// Package tasks Define the deploy task payload and handler
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

// TypeDeployRaw Task deploy raw type
// TypeDeployKnown Task deploy known type
const (
	TypeDeployRaw       = "deploy:raw"
	TypeDeployKnown     = "deploy:known"
	TypeDeployInfoRaw   = "deployinfo:raw"
	TypeDeployInfoKnown = "deployinfo:known"
)

// NewDeployRawTask Used for not yet parsed deploy
func NewDeployRawTask(hash string) (*asynq.Task, error) {
	payload, err := json.Marshal(DeployRawPayload{DeployHash: hash})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeDeployRaw, payload), nil
}

func NewDeployInfoRawTask(hash string, blockHash string, stateRootHash string, deployTimestamp string) (*asynq.Task, error) {

	payload, err := json.Marshal(DeployInfoRawPayload{DeployInfoHash: hash, Block: blockHash, StateRootHash: stateRootHash, DeployTimestamp: deployTimestamp})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeDeployInfoRaw, payload), nil
}

// NewDeployKnownTask used for already parsed deploy
func NewDeployKnownTask(hash string) (*asynq.Task, error) {
	payload, err := json.Marshal(DeployKnownPayload{DeployHash: hash})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeDeployKnown, payload), nil
}

// HandleDeployRawTask fetch a deploy from the rpc endpoint, parse it, and insert it in the database
func HandleDeployRawTask(ctx context.Context, t *asynq.Task) error {
	var p DeployRawPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}

	rpcDeploy, resp, err := WorkerRpcClient.GetDeploy(p.DeployHash)
	if err != nil {
		println("ERROR WorkerRpcClient.GetDeploy(p.DeployHash)")
		fmt.Printf("%v", err)
		return err
	}

	result, cost, errorMessage, err := rpcDeploy.GetResultAndCost()
	if err != nil {
		println("ERROR on rpcDeploy.GetResultAndCost()")
		fmt.Printf("%v", err)
		return err
	}
	metadataDeployType, metadata := rpcDeploy.GetDeployMetadata()
	events := rpcDeploy.GetEvents()
	jsonString := strings.ReplaceAll(string(resp), "\\u0000", "")
	contractHash, _ := rpcDeploy.GetStoredContractHash()
	// TODO: Manque des contrats dans la table contrats, seuls les transform == "WriteContract" sont dans la table
	// Quand on cherche un contrat à partir d'un contratHash de la table Deploy, on ne le retrouve pas à chaque fois
	contractName := rpcDeploy.GetName()
	entrypoint, _ := rpcDeploy.GetEntrypoint()
	var database = db.DB{Postgres: WorkerPool}
	metadata = strings.ReplaceAll(metadata, "\\u0000", "")
	err = database.InsertDeploy(ctx, rpcDeploy.Deploy.Hash, rpcDeploy.Deploy.Header.Account, cost, result, errorMessage, rpcDeploy.Deploy.Header.Timestamp, rpcDeploy.ExecutionResults[0].BlockHash, rpcDeploy.GetType(), jsonString, metadataDeployType, contractHash, contractName, entrypoint, metadata, events)
	if err != nil {
		println("ERROR on database.InsertDeploy")
		fmt.Printf("%v", err)
		return err
	}

	addAccountToQueue(rpcDeploy.Deploy.Header.Account)

	writeContracts := rpcDeploy.GetWriteContract()

	for _, writeContract := range writeContracts {
		addContractToQueue(strings.ReplaceAll(writeContract, "hash-", ""), rpcDeploy.Deploy.Hash, rpcDeploy.Deploy.Header.Account)
	}

	writeContractPackages := rpcDeploy.GetWriteContractPackage()

	for _, writeContractPackage := range writeContractPackages {
		addContractPackageToQueue(strings.ReplaceAll(writeContractPackage, "hash-", ""), rpcDeploy.Deploy.Hash, rpcDeploy.Deploy.Header.Account)
	}
	return nil
}

// HandleDeployRawTask fetch a deploy from the rpc endpoint, parse it, and insert it in the database
func HandleDeployInfoRawTask(ctx context.Context, t *asynq.Task) error {
	var p DeployInfoRawPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}

	var database = db.DB{Postgres: WorkerPool}
	rpcDeployInfo, resp, err := WorkerRpcClient.GetDeployInfo(p.StateRootHash, p.DeployInfoHash)
	if err != nil {
		errdb := database.InsertDeployInfo(ctx, p.DeployInfoHash, p.Block, "", "", 0, "\"ERROR\"", "")
		if errdb != nil {
			fmt.Printf("%v", errdb)
			return errdb
		}
		return err
	}

	strTransfers := strings.Join(rpcDeployInfo.StoredValue.DeployInfo.Transfers, ", ")
	jsonString := strings.ReplaceAll(string(resp), "\\u0000", "")
	gas, err := strconv.Atoi(rpcDeployInfo.StoredValue.DeployInfo.Gas)
	err = database.InsertDeployInfo(ctx, p.DeployInfoHash, p.Block, rpcDeployInfo.StoredValue.DeployInfo.From, rpcDeployInfo.StoredValue.DeployInfo.Source, gas, jsonString, strTransfers)

	if err != nil {
		fmt.Printf("%v", err)
		return err
	}

	for _, transfer := range rpcDeployInfo.StoredValue.DeployInfo.Transfers {
		addTransferToQueue(transfer, p.Block, p.DeployInfoHash, p.DeployTimestamp, p.StateRootHash)
	}

	return nil
}

// HandleDeployKnownTask fetch a deploy from the database, parse it, and insert it in the database
func HandleDeployKnownTask(ctx context.Context, t *asynq.Task) error {
	var p DeployKnownPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}
	var database = db.DB{Postgres: WorkerPool}
	dbDeploy, err := database.GetDeploy(ctx, p.DeployHash)
	if err != nil {
		log.Printf("Can't find deploy %s\n", p.DeployHash)
		return err
	}

	result, cost, errorMessage, err := dbDeploy.GetResultAndCost()
	if err != nil {
		return err
	}
	metadataDeployType, metadata := dbDeploy.GetDeployMetadata()
	events := dbDeploy.GetEvents()
	if metadata != "" {
		log.Printf("New metadata found for %s of type : %s\n", p.DeployHash, metadataDeployType)
		contractHash, _ := dbDeploy.GetStoredContractHash()
		contractName := dbDeploy.GetName()
		entrypoint, _ := dbDeploy.GetEntrypoint()
		metadata = strings.ReplaceAll(metadata, "\\u0000", "")
		err = database.UpdateDeploy(ctx, dbDeploy.Deploy.Hash, dbDeploy.Deploy.Header.Account, cost, result, errorMessage, dbDeploy.Deploy.Header.Timestamp, dbDeploy.ExecutionResults[0].BlockHash, dbDeploy.GetType(), metadataDeployType, contractHash, contractName, entrypoint, metadata, events)
		if err != nil {
			return err
		}
		writeContractPackages := dbDeploy.GetWriteContractPackage()

		for _, writeContractPackage := range writeContractPackages {
			addContractPackageToQueue(strings.ReplaceAll(writeContractPackage, "hash-", ""), dbDeploy.Deploy.Hash, dbDeploy.Deploy.Header.Account)
		}

		writeContracts := dbDeploy.GetWriteContract()

		for _, writeContract := range writeContracts {
			addContractToQueue(strings.ReplaceAll(writeContract, "hash-", ""), dbDeploy.Deploy.Hash, dbDeploy.Deploy.Header.Account)
		}
	}
	return nil
}

// addDeployToQueue a deploy hash to the queue
func addContractToQueue(hash string, deployhash string, from string) {
	task, err := NewContractRawTask(hash, deployhash, from)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("contracts"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

// addDeployToQueue a deploy hash to the queue
func addContractPackageToQueue(hash string, deployhash string, from string) {
	task, err := NewContractPackageRawTask(hash, deployhash, from)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("contracts"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

// addAccountToQueue add a account publicKey to the queue
func addAccountToQueue(publicKey string) {
	task, err := NewAccountTask(publicKey)
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	_, err = WorkerAsyncClient.Enqueue(task, asynq.Queue("accounts"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
}

type DeployRawPayload struct {
	DeployHash string
}

type DeployKnownPayload struct {
	DeployHash string
}

type DeployInfoRawPayload struct {
	DeployInfoHash  string
	Block           string
	StateRootHash   string
	DeployTimestamp string
}

type DeployInfoKnownPayload struct {
	DeployInfoHash string
	Block          string
	StateRootHash  string
}
