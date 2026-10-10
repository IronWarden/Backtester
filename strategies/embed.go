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

// UserPrefix marks a script as the user's own rather than the library's:
// the Lua tab's scratch file is my_strategy.lua, and "Edit a copy" of a library
// script saves it as my_<name>.lua. A user script saved into strategies/ is
// compiled into the next build like everything else there, so the prefix, not
// the embedding, is what tells the two apart.
const UserPrefix = "my_"

// IsLibrary reports whether path names a shipped library script — one the app
// shows read-only and never writes, so an edit cannot break what the gallery
// and every config naming it run. path may be bare ("rsi.lua"), relative
// ("../strategies/rsi.lua") or absolute; what counts is that it sits in a
// strategies directory, is in the library, and is not a user's my_ script.
func IsLibrary(path string) bool {
	p := strings.ReplaceAll(path, "\\", "/")
	dir, base := "", p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		dir, base = p[:i], p[i+1:]
	}
	if strings.HasPrefix(base, UserPrefix) {
		return false
	}
	if dir != "" && dir != Dir && !strings.HasSuffix(dir, "/"+Dir) {
		return false
	}
	_, ok := Source(base)
	return ok
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
