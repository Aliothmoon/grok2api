// Package json is the project-wide JSON codec facade over bytedance/sonic.
//
// Prefer this package for Marshal/Unmarshal/Encoder/Decoder on hot paths.
// Paths that need encoding/json.Decoder.Token, InputOffset, or stdlib stream
// recovery semantics (credential multi-value import, media token scanners)
// may import encoding/json directly — those are intentional exceptions.
//
// ConfigStd keeps wire behavior close to encoding/json (EscapeHTML, SortMapKeys,
// CopyString). RawMessage/Number alias encoding/json types for identity.
package json

import (
	stdjson "encoding/json"
	"io"

	"github.com/bytedance/sonic"
)

// api is sonic ConfigStd: std-compatible EscapeHTML + SortMapKeys.
// Prefer this over package-level sonic.Marshal (which uses ConfigDefault).
var api = sonic.ConfigStd

// RawMessage is an alias of encoding/json.RawMessage for type identity with
// existing struct fields and APIs. Encoding/decoding uses sonic via MarshalJSON
// hooks on the underlying type when embedded in larger values.
type RawMessage = stdjson.RawMessage

// Number is an alias of encoding/json.Number for UseNumber / interface{} numbers.
type Number = stdjson.Number

// Delim is retained for streaming token parsers that still need delimiter kinds
// when walking values decoded via Unmarshal into generic containers.
type Delim = stdjson.Delim

// SyntaxError is the stdlib syntax error type. Prefer errors.As against it when
// present; sonic may surface different error types on invalid input.
type SyntaxError = stdjson.SyntaxError

// Marshal returns the JSON encoding of v using sonic ConfigStd.
func Marshal(v any) ([]byte, error) {
	return api.Marshal(v)
}

// MarshalIndent is like Marshal but applies indentation.
func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return api.MarshalIndent(v, prefix, indent)
}

// Unmarshal parses JSON-encoded data into v using sonic ConfigStd.
func Unmarshal(data []byte, v any) error {
	return api.Unmarshal(data, v)
}

// Valid reports whether data is valid JSON.
func Valid(data []byte) bool {
	return api.Valid(data)
}

// NewEncoder returns a streaming encoder writing to w.
func NewEncoder(w io.Writer) sonic.Encoder {
	return api.NewEncoder(w)
}

// NewDecoder returns a streaming decoder reading from r.
func NewDecoder(r io.Reader) sonic.Decoder {
	return api.NewDecoder(r)
}
