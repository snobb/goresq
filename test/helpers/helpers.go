package helpers

import (
	"encoding/json"
)

// Marshal data into json
func Marshal(payload interface{}) []byte {
	res, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	return res
}
