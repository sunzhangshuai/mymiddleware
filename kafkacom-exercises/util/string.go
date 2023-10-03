package util

import (
	"encoding/json"
)

func FmtString(obj interface{}) string {
	b, _ := json.MarshalIndent(obj, "", "  ")
	return string(b)
}
