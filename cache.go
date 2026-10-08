package goose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// PersistentCache stores the SDK's last-known-good state so a process that
// restarts during a config-service outage can serve real values immediately
// instead of booting with an empty cache. The SDK reads it once on Connect
// (before any network I/O) and writes a debounced snapshot as values change.
// Implementations must be safe for concurrent Load/Save calls; the SDK
// serializes its own calls but callers may share an instance.
type PersistentCache interface {
	// Load returns the last saved snapshot, or (nil, nil) when nothing has been
	// persisted yet. A non-nil error is logged and surfaced to OnError but never
	// aborts startup.
	Load(ctx context.Context) (*Snapshot, error)
	// Save persists a snapshot, replacing any previous one.
	Save(ctx context.Context, snapshot *Snapshot) error
}

// Snapshot is the JSON-serializable last-known-good state of a client: enough to
// answer flag and config reads, and to resume incremental polling, without the
// network. Maps are keyed by flagset (then flag key) or by config name.
type Snapshot struct {
	Flags         map[string]map[string]FlagValue       `json:"flags"`
	FlagDataTypes map[string]map[string]string          `json:"flagDataTypes"`
	Rollouts      map[string]map[string]RolloutSnapshot `json:"rollouts"`
	PollCursors   map[string]int64                      `json:"pollCursors"`
	Configs       map[string]ConfigSnapshot             `json:"configs"`
}

// RolloutSnapshot is the serializable form of a flag's canary rollout config.
type RolloutSnapshot struct {
	Percentage *int      `json:"percentage,omitempty"`
	Salt       string    `json:"salt,omitempty"`
	Value      FlagValue `json:"value,omitempty"`
}

// ConfigSnapshot is the serializable form of a watched config document.
type ConfigSnapshot struct {
	Document map[string]any `json:"document"`
	Revision int            `json:"revision"`
}

// FileCache is a PersistentCache backed by a single JSON file. Writes are atomic
// (temp file + rename) so a crash mid-write never leaves a corrupt cache. A
// missing file loads as an empty (nil) snapshot.
type FileCache struct {
	path string
}

// NewFileCache returns a FileCache that persists to path.
func NewFileCache(path string) *FileCache {
	return &FileCache{path: path}
}

// Load reads and decodes the snapshot. A missing file yields (nil, nil).
func (f *FileCache) Load(ctx context.Context) (*Snapshot, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	// Standard decoding of interface{} yields float64 for JSON numbers, which is
	// exactly the numeric representation the rest of the SDK expects.
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// Save atomically writes the snapshot as pretty-printed JSON.
func (f *FileCache) Save(ctx context.Context, snapshot *Snapshot) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".goose-cache-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpName, f.path)
}
