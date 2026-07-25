package json_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/chenyme/grok2api/backend/internal/pkg/json"
)

type sampleDTO struct {
	ID    uint64            `json:"id"`
	Name  string            `json:"name"`
	Meta  map[string]string `json:"meta,omitempty"`
	Flags []bool            `json:"flags"`
	Raw   json.RawMessage   `json:"raw,omitempty"`
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	in := sampleDTO{
		ID: 42, Name: "account", Meta: map[string]string{"tier": "free"},
		Flags: []bool{true, false}, Raw: json.RawMessage(`{"k":1}`),
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Fatalf("marshal produced invalid JSON: %s", data)
	}
	var out sampleDTO
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Name != in.Name || out.Meta["tier"] != "free" || len(out.Flags) != 2 || !out.Flags[0] {
		t.Fatalf("round-trip mismatch: %#v", out)
	}
	if string(out.Raw) != `{"k":1}` && string(out.Raw) != `{"k": 1}` {
		var nested map[string]any
		if err := json.Unmarshal(out.Raw, &nested); err != nil || nested["k"] == nil {
			t.Fatalf("raw field = %s", out.Raw)
		}
	}
}

func TestDecoderEncoderStreaming(t *testing.T) {
	payload := []byte(`{"id":7,"name":"x","flags":[true]}`)
	dec := json.NewDecoder(bytes.NewReader(payload))
	var value sampleDTO
	if err := dec.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if value.ID != 7 || value.Name != "x" {
		t.Fatalf("decoded %#v", value)
	}
	// Production decodeSingleJSON uses errors.Is(err, io.EOF).
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after single value, got %v", err)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(map[string]any{"ok": true, "n": 1}); err != nil {
		t.Fatal(err)
	}
	out := buf.Bytes()
	if !json.Valid(bytes.TrimSpace(out)) {
		t.Fatalf("encoder output invalid: %q", out)
	}
	// stdlib-compatible Encoder.Encode appends a trailing newline.
	if len(out) == 0 || out[len(out)-1] != '\n' {
		t.Fatalf("Encode should append trailing newline, got %q", out)
	}
}

func TestMarshalIndent(t *testing.T) {
	data, err := json.MarshalIndent(map[string]int{"a": 1}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("\n")) || !json.Valid(data) {
		t.Fatalf("indent output = %s", data)
	}
}

// TestFacadeIsSonicConfigStd exercises the real shipped Marshal/Unmarshal entry
// points used by HTTP/settings/provider code (not a reimplementation).
func TestFacadeIsSonicConfigStd(t *testing.T) {
	data, err := json.Marshal(map[string]string{"x": "<script>"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("<script>")) {
		t.Fatalf("ConfigStd should EscapeHTML, got %s", data)
	}
	if !bytes.Contains(data, []byte(`\u003c`)) && !bytes.Contains(data, []byte(`\u003C`)) {
		t.Fatalf("expected escaped HTML in %s", data)
	}
	var out map[string]string
	if err := json.Unmarshal(data, &out); err != nil || out["x"] != "<script>" {
		t.Fatalf("unmarshal = %#v err=%v", out, err)
	}
}

func TestConfigStdSortsMapKeys(t *testing.T) {
	// ConfigStd enables SortMapKeys — keys should appear in sorted order.
	data, err := json.Marshal(map[string]int{"z": 1, "a": 2, "m": 3})
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	ia, iz := strings.Index(s, `"a"`), strings.Index(s, `"z"`)
	if ia < 0 || iz < 0 || ia > iz {
		t.Fatalf("expected sorted keys a before z, got %s", s)
	}
}

func TestDisallowUnknownFields(t *testing.T) {
	type strict struct {
		ID uint64 `json:"id"`
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(`{"id":1,"extra":true}`)))
	dec.DisallowUnknownFields()
	var value strict
	if err := dec.Decode(&value); err == nil {
		t.Fatal("expected error for unknown field")
	}
	// Known-only payload must succeed.
	dec = json.NewDecoder(bytes.NewReader([]byte(`{"id":2}`)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&value); err != nil || value.ID != 2 {
		t.Fatalf("strict decode = %#v err=%v", value, err)
	}
}
