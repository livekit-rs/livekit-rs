// Actual unmodified pinned go-homedir v1.1.0 + Go POSIX filepath implementation.
package main

import (
	"encoding/hex"
	"encoding/json"
	"github.com/mitchellh/go-homedir"
	"os"
)

type row struct {
	Name string `json:"name"`
	Home string `json:"home_hex"`
	Path string `json:"path_hex"`
}

func main() {
	var rows []row
	if err := json.NewDecoder(os.Stdin).Decode(&rows); err != nil {
		panic(err)
	}
	enc := json.NewEncoder(os.Stdout)
	for _, r := range rows {
		h, e := hex.DecodeString(r.Home)
		if e != nil {
			panic(e)
		}
		p, e := hex.DecodeString(r.Path)
		if e != nil {
			panic(e)
		}
		if e = os.Setenv("HOME", string(h)); e != nil {
			panic(e)
		}
		homedir.Reset()
		v, e := homedir.Expand(os.ExpandEnv(string(p)))
		errText := ""
		if e != nil {
			errText = e.Error()
		}
		if e = enc.Encode(map[string]string{"name": r.Name, "path_hex": hex.EncodeToString([]byte(v)), "error": errText}); e != nil {
			panic(e)
		}
	}
}
