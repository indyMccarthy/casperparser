# CasperParser - A parser for the Casper Blockchain


[![codecov](https://codecov.io/gh/casperholders/casperparser/branch/main/graph/badge.svg?token=FZMG84DN4H)](https://codecov.io/gh/casperholders/casperparser)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=casperholders_casperparser&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=casperholders_casperparser)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=casperholders_casperparser&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=casperholders_casperparser)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=casperholders_casperparser&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=casperholders_casperparser)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=casperholders_casperparser&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=casperholders_casperparser)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=casperholders_casperparser&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=casperholders_casperparser)

Built with :
- [Go 1.18](https://go.dev/)
- [Viper](https://github.com/spf13/viper)
- [Cobra](https://github.com/spf13/cobra)
- [Asynq](https://github.com/hibiken/asynq)
- [PGX](https://github.com/jackc/pgx)

# Prerequisites

This software use the library Asynq and PGX.  
You need a Redis instance / cluster / sentinel HA to enqueue tasks.

Docker local instance example : 

```bash
docker run --name redis -p 127.0.0.1:6379:6379 -d redis
```

You need a Postgresql instance to store the data generated by the software.

Docker local instance example : 

```bash
docker run --name some-postgres -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=casperparser -d postgres
```

## Optional add-on

The software will directly apply the migration but if you want you can use the migrate cli to apply the migration to the database :

```bash
make run_migrate
```

There's a web UI for the lib Asynq called [Asynqmon](https://github.com/hibiken/asynqmon).

```bash
docker run --rm --name asynqmon -p 127.0.0.1:8080:8080 -d hibiken/asynqmon
```

Or you can install the asynq lib locally because it's also a CLI that provide a dashboard to monitor your queues :

```bash
go install github.com/hibiken/asynq/tools/asynq
```

And then run asynq :

```bash
asynq
```

And then finally if you want to setup a REST API with the data generated with this parser you can use [Postgrest](https://postgrest.org/en/stable/).

```bash
docker run --rm --net=host -p 3000:3000 \
  -e PGRST_DB_URI="postgres://postgres@localhost/postgres" \
  -e PGRST_DB_SCHEMAS="casperparser" \
  -e PGRST_DB_ANON_ROLE="postgres" \
  postgrest/postgrest
```

## Database Schema

Here's the database Schema.

Four tables, blocks & raw blocks (Contains all parsed blocks and raw blocks) and deploys & raw deploys (Contains all parsed deploys and raw deploys).  
The block data structure contain :

- The hash of the block
- The era
- The timestamp
- The height
- A boolean era_end to indicate if the block is a switch block
- A boolean validated to indicate if all deploys contained inside the block are correctly inserted into the database.

The raw block data structure contain :
- The hash of the block
- The raw json rpc response

The deploy data structure contain :

- The hash of the deploy
- The address from which the deploy is initiated (from)
- The cost in motes
- The result of the deploy as a boolean (true = success / false = failed)
- The timestamp
- The block hash of the deploy
- The type of the deploy (transfer or session type)
- The metadata_type an approximation about the deploy content
- The metadata contains a json with the args values parsed into a string
- The events contain all the events of the deploys if any

A deploy is linked to a block through a block hash and the 'from' column is indexed to speed up querying by addresses.

The raw deploy data structure contain :
- The hash of the deploy
- The raw json rpc response

There's an additional view included within the database : full_stats. This view expose the last 14 days stats about
deploys types.

![Database Schema](diagram.png "Database Schema")

# How to build

## Local dev

```bash
make build
```

This will produce a single binary at the project root folder.

## Local tests

### !! Important !!

In order to run correctly the tests locally you need to have a .env file and a .casperParser.yaml file.  
You can use those provided as an example and replace the different values needed. 

```
CASPER_PARSER_REDIS=127.0.0.1:6379 //Redis single instance connection string
CASPER_PARSER_RPC=http://127.0.0.1:7777/rpc //Node rpc endpoint
CASPER_PARSER_DATABASE=postgres://postgres:myPassword@localhost:5432/gotest //DB Connexion string
CASPER_PARSER_EVENT=http://127.0.0.1:9999/events/main //Node events endpoint
```

### Run tests
```bash
make coverage
```

This will run the tests and open a webpage with the coverage results.

## Documentation

The code is documented like all the commands of the CLI.
Simply use the CLI with the flag `--help` to display the user documentation.

For the project documentation if you want to have a web page with a standard godoc documentation install godoc :

```bash
go install golang.org/x/tools/cmd/godoc 
```

And then run godoc : 


```bash
godoc -http=:6060 
```

You can now access the documentation at http://localhost:6060/

## Api endpoints

No api endpoint are available with this software.

# Configuration

You can configure the CLI by either the config file, env variables or flags. Or a combination of those three options.
The CLI flags will override the env variables keys, and the env variables will override the config file keys.

## Config File explanation

```
rpc: "node_url_rpc_endpoint" (Provide your node RPC endpoint)
database: "postgres://postgres:myPassword@localhost:5432/casperparser" (Provide your database connexion string, prefer the env variable for that)
redis: "127.0.0.1:6379" (Redis single instance address)
cluster: ["REDIS_INSTANCE_URL1", "REDIS_INSTANCE_URL2"] (Will override the redis key and use the cluster key instead for an HA Redis)
sentinel:  ["REDIS_SENTINEL_URL1", "REDIS_SENTINEL_URL2"] (Will override the redis and cluster key and use the sentinel key instead for a HA/Failover Redis) 
master: "master_redis_sentinel_name" (Required when the sentinel key is used, default value : mymaster)
concurrency: 100 //Number of concurrent workers that will be spwaned with the worker command
deploysTypes:
  storedContracts:
    [category]: (Just needed to seperate deploy that have identical entrypoints)
        [entrypoint]:
            deployName: "metadata_type" (Will be used as the metadata_type in the db)
            hasName: [true/false] (Has a name or not)
            contractName: "name" (Required if hasName is true)
            args: ["arg1", "arg2"] (Array of string containing the args name to find)
  moduleBytes:
    [deployName]: (Will be used as the metadata_type in the db)
        strictArgs: [true/false - Will check the exact number of args]
        args: ["arg1", "arg2"] (Array of string containing the args name to find)
```

## Env File explanation

```
CASPER_PARSER_REDIS=127.0.0.1:6379 //(Redis single instance address)
CASPER_PARSER_RPC=http://localhost:8080/rpc //(Provide your node RPC endpoint)
CASPER_PARSER_DATABASE=postgres://postgres:myPassword@localhost:5432/casperparser //Provide your database connexion string
CASPER_PARSER_CLUSTER=REDIS_INSTANCE_URL1,REDIS_INSTANCE_URL2 //(Will override the redis key and use the cluster key instead for an HA Redis)
CASPER_PARSER_SENTINEL=REDIS_SENTINEL_URL1,REDIS_SENTINEL_URL2 //(Will override the redis and cluster key and use the sentinel key instead for a HA/Failover Redis)
CASPER_PARSER_MASTER=60 //(Required when the sentinel key is used, default value : mymaster)
CASPER_PARSER_CONCURRENCY=100 //Number of concurrent workers that will be spwaned with the worker command
```

## Docker build

```bash
docker build . 
```

## Kubernetes deployment

### Warning: The current kubernetes files are specific to my kubenertes architecture. It's basically an example how to use CasperHolders on Kubernetes.

```bash
kubectl apply -f kubernetes/(testnet|mainnet)/
```