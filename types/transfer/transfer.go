// Package transfer provide struct and object methods to interact with transfers from the Casper Blockchain
package transfer

import (
	"fmt"
	"math/big"
	"strconv"
)

// getValue return the string value of an argument value
func getValue(v interface{}) interface{} {
	if unboxed, ok := v.(map[string]interface{}); ok {
		datas := make(map[string]interface{})

		for key, value := range unboxed {
			datas[key] = getValue(value)
		}
		return datas
	}
	if unboxed, ok := v.([]interface{}); ok {
		return unboxed
	}
	if unboxed, ok := v.(map[int]interface{}); ok {
		datas := make(map[string]interface{})

		for key, value := range unboxed {
			datas[fmt.Sprint(key)] = getValue(value)
		}
		return datas
	}
	switch v.(type) {
	case nil:
		return ""
	case bool:
		return strconv.FormatBool(v.(bool))
	case float64:
		return strconv.Itoa(int(v.(float64)))
	case int:
		return strconv.Itoa(v.(int))
	case string:
		return v.(string)
	default:
		return fmt.Sprintf("%v", v)
	}
}

type Result struct {
	ApiVersion  string              `json:"api_version"`
	BlockHeader JsonBlockHeader     `json:"block_header"`
	StoredValue TransferStoredValue `json:"stored_value"`
}

type JsonBlockHeader struct {
	ParentHash      string `json:"parent_hash"`
	StateRootHash   string `json:"state_root_hash"`
	BodyHash        string `json:"body_hash"`
	RandomBit       bool   `json:"random_bit"`
	AccumulatedSeed string `json:"accumulated_seed"`
	Timestamp       string `json:"timestamp"`
	EraID           int    `json:"era_id"`
	Height          int    `json:"height"`
	ProtocolVersion string `json:"protocol_version"`
	EraEnd          *struct {
		EraReport struct {
			Equivocators []string `json:"equivocators"`
			Rewards      []struct {
				Validator string  `json:"validator"`
				Amount    big.Int `json:"amount"`
			} `json:"rewards"`
			InactiveValidators []string `json:"inactiveValidators"`
		} `json:"era_report"`
		NextEraValidatorWeights []struct {
			Validator string `json:"validator"`
			Weight    string `json:"weight"`
		} `json:"next_era_validator_weights"`
	} `json:"era_end"`
}

type TransferStoredValue struct {
	Transfer struct {
		Deploy string `json:"deploy_hash"`
		From   string `json:"from"`
		To     string `json:"to"`
		Source string `json:"source"`
		Target string `json:"target"`
		Amount string `json:"amount"`
		Gas    string `json:"gas"`
		Id     int    `json:"id"`
	} `json:"Transfer"`
}
