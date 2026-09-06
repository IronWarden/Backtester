// Package strategies embeds the shipped Lua strategy library into the binary,
// the way ui embeds frontend/dist. Before this the library was a directory
// the app hoped to find next to itself: launched from anywhere but the
// checkout, the gallery was empty and every config saying
// Strategy = "lua:strategies/sma_cross.lua" failed to resolve.
//
// The embedded copy is the FALLBACK, never the first answer. An on-disk
// library wins wherever one exists (see src/apppath), so editing a script in
// the Lua tab keeps working in a checkout, and a script saved into the
// per-user library shadows the built-in one of the same name.
package strategies

import (
	"embed"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.lua
var files embed.FS

// FS is the embedded library as a filesystem, flat: the scripts sit at its
// root, named "sma_cross.lua" rather than "strategies/sma_cross.lua".
var FS fs.FS = files

// Dir is the name configs refer to the library by. A portfolio writes
// Strategy = "lua:strategies/foo.lua" whether the file is on disk or in the
// binary, so nothing a user has written changes when this package is used.
const Dir = "strategies"

// Source returns the embedded script for a path written the way a config
// writes it -- "strategies/foo.lua", "./strategies/foo.lua" or bare
// "foo.lua" -- and whether the library has it. Any path that reaches outside
// the library is not one of ours, so it simply misses.
func Source(path string) ([]byte, bool) {
	name, ok := libraryName(path)
	if !ok {
		return nil, false
	}
	b, err := files.ReadFile(name)
	if err != nil {
		return nil, false
	}
	return b, true
}

// libraryName reduces a config's path to the embedded file's name, rejecting
// anything that is not a plain script directly inside the library.
func libraryName(path string) (string, bool) {
	p := strings.TrimPrefix(strings.ReplaceAll(path, "\\", "/"), "./")
	p = strings.TrimPrefix(p, Dir+"/")
	if p == "" || strings.Contains(p, "/") || !strings.HasSuffix(p, ".lua") {
		return "", false
	}
	return p, true
}

// Names returns the embedded scripts' filenames, sorted, so a caller can list
// the library without knowing it is embedded.
func Names() []string {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".lua") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}
