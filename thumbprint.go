package goose

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func resolveThumbprint(explicit, clientID string) string {
	if normalized := strings.TrimSpace(explicit); normalized != "" {
		return normalized
	}

	hostname, _ := os.Hostname()
	executable, err := os.Executable()
	if err == nil {
		executable = filepath.Base(executable)
	}

	seed := strings.Join([]string{"go", clientID, hostname, executable}, "|")
	if strings.Trim(seed, "|") == "" {
		seed = fmt.Sprintf("go|%s|%d", clientID, time.Now().UnixNano())
	}

	sum := sha256.Sum256([]byte(seed))
	return "gth_" + hex.EncodeToString(sum[:])
}
