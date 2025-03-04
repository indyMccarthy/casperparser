// Package cmd Define the worker command with the Cobra CLI library
package cmd

import (
	"casperParser/db"
	"casperParser/tasks"
	"context"
	"log"
	"strconv"

	"github.com/hibiken/asynq"

	"github.com/spf13/cobra"
)

var concurrency int
var queues []string

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start a new worker",
	Long: `Start a worker
Usage examples :
casperParser worker -- Will start the worker with the default values (Either from your config file or defined in the program)
casperParser worker --queues blocks,1 --concurrency 20 -- Will start the worker to handle only blocks and 20 concurrent workers
casperParser worker --redis 127.0.0.1:6379 -- Will start the worker with a single redis server connection (Use ENV variables preferably to setup a secure redis connection)
	`,
	Run: func(cmd *cobra.Command, args []string) {
		conf := asynq.Config{
			Concurrency: concurrency,
			Queues: map[string]int{
				"blocks":      1,
				"deploys":     1,
				"deployinfos": 1,
				"transfers":   1,
				"contracts":   1,
				"era":         1,
				"auction":     1,
				"auctionera":  1,
				"accounts":    1,
			},
		}
		tasks.WorkerRpcClient = getRpcClient()
		if cmd.Flags().Lookup("queues").Changed {
			queuesMap := make(map[string]int)
			if len(queues)%2 != 0 {
				log.Fatalf("Can't parse queues flag. Usage --queues [queueName1],[priority1],[queue2],[priority2]")
				return
			}
			for i := 0; i < len(queues); i += 2 {
				var err error
				if queues[i] != "blocks" && queues[i] != "deploys" && queues[i] != "deployinfos" && queues[i] != "transfers" && queues[i] != "contracts" && queues[i] != "era" && queues[i] != "auction" && queues[i] != "auctionera" {
					log.Fatalf("Unknown queue %s. Supported queues : blocks, deploys, deployinfos, transfers, contracts, era, auction, auctionera, accounts", queues[i])
				}
				queuesMap[queues[i]], err = strconv.Atoi(queues[i+1])
				if err != nil {
					log.Fatalf("Can't parse queues flag. Usage --queues [queueName1],[priority1],[queue2],[priority2]")
					return
				}
			}
			conf.Queues = queuesMap
		}
		log.Printf("Concurrency : %v\n", conf.Concurrency)
		log.Printf("Queue config used : %v\n", conf.Queues)
		startWorkers(getRedisConf(cmd), conf)
	},
}

// init the command flags
func init() {
	RootCmd.AddCommand(workerCmd)
	workerCmd.Flags().IntVarP(&concurrency, "concurrency", "k", 100, "Number of concurrent workers to use. The database connection pool will be set to the same number")
	workerCmd.Flags().StringSliceVarP(&queues, "queues", "q", []string{"blocks", "1", "deploys", "1", "deployinfos", "1", "transfers", "1", "contracts", "1", "era", "1", "auction", "1", "auctionera", "1", "accounts", "1"}, "Set queues with priority")
}

// startWorkers with a redis and asynq config
func startWorkers(redis asynq.RedisConnOpt, conf asynq.Config) {
	var err error
	tasks.WorkerPool, err = db.NewPGXPool(context.Background(), getDatabaseConnectionString(), conf.Concurrency)
	defer tasks.WorkerPool.Close()
	srv := asynq.NewServer(
		redis,
		conf,
	)
	if err != nil {
		log.Fatalln(err)
	}
	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.TypeBlockRaw, tasks.HandleBlockRawTask)
	mux.HandleFunc(tasks.TypeBlockVerify, tasks.HandleBlockVerifyTask)
	mux.HandleFunc(tasks.TypeDeployRaw, tasks.HandleDeployRawTask)
	mux.HandleFunc(tasks.TypeDeployInfoRaw, tasks.HandleDeployInfoRawTask)
	mux.HandleFunc(tasks.TypeDeployKnown, tasks.HandleDeployKnownTask)
	mux.HandleFunc(tasks.TypeTransferRaw, tasks.HandleTransferRawTask)
	mux.HandleFunc(tasks.TypeTransferKnown, tasks.HandleTransferKnownTask)
	mux.HandleFunc(tasks.TypeContractPackageRaw, tasks.HandleContractPackageRawTask)
	mux.HandleFunc(tasks.TypeContractRaw, tasks.HandleContractRawTask)
	mux.HandleFunc(tasks.TypeReward, tasks.HandleRewardTask)
	mux.HandleFunc(tasks.TypeAuction, tasks.HandleAuctionTask)
	mux.HandleFunc(tasks.TypeAuctionEra, tasks.HandleAuctionEraTask)
	mux.HandleFunc(tasks.TypeAccountHash, tasks.HandleAccountHashTask)
	mux.HandleFunc(tasks.TypeAccountPublicKey, tasks.HandleAccountTask)
	mux.HandleFunc(tasks.TypeAccountUref, tasks.HandlePurseTask)
	mux.HandleFunc(tasks.TypeAccountFetch, tasks.HandleFetchPurseTask)
	tasks.WorkerAsyncClient = asynq.NewClient(redis)
	defer tasks.WorkerAsyncClient.Close()
	if err := srv.Run(mux); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}
