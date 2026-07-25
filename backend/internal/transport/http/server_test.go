package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/transport/http/middleware"
)

func testDependencies() Dependencies {
	return Dependencies{RequestTimeout: time.Second, MaxBodyBytes: 1024, ConcurrencyGate: middleware.NewConcurrencyGate(1024)}
}

func TestReadinessEndpointReturnsStructuredDegradedStateAsReady(t *testing.T) {
	deps := testDependencies()
	deps.Readiness = func(context.Context) ReadinessSnapshot {
		return ReadinessSnapshot{
			Ready: true, State: "degraded", UpdatedAt: time.Now().UTC(),
			Components: map[string]ReadinessComponent{
				"grok_build": {State: "ready"},
				"grok_web":   {State: "unavailable"},
			},
		}
	}
	router := New(deps)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var body ReadinessSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Ready || body.State != "degraded" || body.Components["grok_build"].State != "ready" {
		t.Fatalf("body = %#v", body)
	}
}

func TestReadinessEndpointReturns503WhileReconciling(t *testing.T) {
	deps := testDependencies()
	deps.Readiness = func(context.Context) ReadinessSnapshot {
		return ReadinessSnapshot{Ready: false, State: "reconciling", UpdatedAt: time.Now().UTC()}
	}
	router := New(deps)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"state":"reconciling"`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestInferenceTrafficIsRejectedWhileReconciling(t *testing.T) {
	deps := testDependencies()
	deps.TrafficReady = func() bool { return false }
	router := New(deps)
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"service_reconciling"`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSystemEndpointsRequireAdminAuthentication(t *testing.T) {
	deps := testDependencies()
	deps.PublicAPIBaseURL = "https://api.example.com"
	router := New(deps)
	for _, route := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/admin/v1/system"},
		{method: http.MethodGet, path: "/api/admin/v1/system/version"},
		{method: http.MethodPost, path: "/api/admin/v1/system/update/check"},
	} {
		request := httptest.NewRequest(route.method, route.path, nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s status = %d, want %d", route.method, route.path, recorder.Code, http.StatusUnauthorized)
		}
	}
}

func TestFrontendStaticFilesAndSPAFallback(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("<html>app</html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "assets", "app.js"), []byte("console.log('app')"), 0o600); err != nil {
		t.Fatal(err)
	}
	deps := testDependencies()
	deps.Logger = slog.Default()
	deps.FrontendStaticPath = root
	router := New(deps)

	for _, test := range []struct {
		path        string
		status      int
		body        string
		cachePrefix string
	}{
		{path: "/assets/app.js", status: http.StatusOK, body: "console.log('app')", cachePrefix: "public"},
		{path: "/dashboard", status: http.StatusOK, body: "<html>app</html>", cachePrefix: "no-cache"},
		{path: "/assets/missing.js", status: http.StatusNotFound},
		{path: "/api/admin/v1/missing", status: http.StatusNotFound},
		{path: "/swagger/index.html", status: http.StatusNotFound},
	} {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d", recorder.Code, test.status)
			}
			if test.body != "" && !strings.Contains(recorder.Body.String(), test.body) {
				t.Fatalf("body = %q", recorder.Body.String())
			}
			if test.cachePrefix != "" && !strings.HasPrefix(recorder.Header().Get("Cache-Control"), test.cachePrefix) {
				t.Fatalf("cache-control = %q", recorder.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestSwaggerRegistrationFollowsStartupConfig(t *testing.T) {
	disabledDeps := testDependencies()
	disabledDeps.Logger = slog.Default()
	disabled := New(disabledDeps)
	disabledRequest := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	disabledRecorder := httptest.NewRecorder()
	disabled.ServeHTTP(disabledRecorder, disabledRequest)
	if disabledRecorder.Code != http.StatusNotFound {
		t.Fatalf("disabled swagger status = %d, want %d", disabledRecorder.Code, http.StatusNotFound)
	}

	enabledDeps := testDependencies()
	enabledDeps.Logger = slog.Default()
	enabledDeps.SwaggerEnabled = true
	enabled := New(enabledDeps)
	enabledRequest := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	enabledRecorder := httptest.NewRecorder()
	enabled.ServeHTTP(enabledRecorder, enabledRequest)
	if enabledRecorder.Code != http.StatusOK {
		t.Fatalf("enabled swagger status = %d, want %d", enabledRecorder.Code, http.StatusOK)
	}
	var document struct {
		Info struct {
			Title string `json:"title"`
		} `json:"info"`
	}
	if err := json.Unmarshal(enabledRecorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode swagger document: %v", err)
	}
	if document.Info.Title != "Grok2API" {
		t.Fatalf("swagger title = %q, want %q", document.Info.Title, "Grok2API")
	}
}

func TestPprofRegistrationFollowsStartupConfig(t *testing.T) {
	disabledDeps := testDependencies()
	disabledDeps.Logger = slog.Default()
	disabled := New(disabledDeps)
	disabledRequest := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	disabledRecorder := httptest.NewRecorder()
	disabled.ServeHTTP(disabledRecorder, disabledRequest)
	if disabledRecorder.Code != http.StatusNotFound {
		t.Fatalf("disabled pprof status = %d, want %d body=%s", disabledRecorder.Code, http.StatusNotFound, disabledRecorder.Body.String())
	}

	enabledDeps := testDependencies()
	enabledDeps.Logger = slog.Default()
	enabledDeps.PprofEnabled = true
	// Ensure SPA catch-all cannot swallow pprof when a frontend build is present.
	staticRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(staticRoot, "index.html"), []byte("<!doctype html><title>spa</title>"), 0o600); err != nil {
		t.Fatal(err)
	}
	enabledDeps.FrontendStaticPath = staticRoot
	enabled := New(enabledDeps)

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine", "/debug/pprof/heap"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recorder := httptest.NewRecorder()
		enabled.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("enabled pprof %s status = %d, want %d body=%s", path, recorder.Code, http.StatusOK, recorder.Body.String())
		}
		if strings.Contains(recorder.Body.String(), "<!doctype html>") {
			t.Fatalf("enabled pprof %s returned SPA HTML", path)
		}
	}
}

func TestBackendPathsIncludeDebugPrefix(t *testing.T) {
	if !isBackendPath("/debug/pprof/goroutine") {
		t.Fatal("expected /debug/pprof/goroutine to be treated as a backend path")
	}
	if !isBackendPath("/debug/cache/stats") {
		t.Fatal("expected /debug/cache/stats to be treated as a backend path")
	}
}

func TestCacheStatsRegistrationFollowsPprofGate(t *testing.T) {
	disabledDeps := testDependencies()
	disabledDeps.Logger = slog.Default()
	disabledDeps.CacheStats = func() any { return map[string]any{"assembled": map[string]any{"hits": 1}} }
	disabled := New(disabledDeps)
	disabledReq := httptest.NewRequest(http.MethodGet, "/debug/cache/stats", nil)
	disabledRec := httptest.NewRecorder()
	disabled.ServeHTTP(disabledRec, disabledReq)
	if disabledRec.Code != http.StatusNotFound {
		t.Fatalf("pprof disabled: cache stats status=%d want 404 body=%s", disabledRec.Code, disabledRec.Body.String())
	}

	var resetCalls int
	enabledDeps := testDependencies()
	enabledDeps.Logger = slog.Default()
	enabledDeps.PprofEnabled = true
	enabledDeps.CacheStats = func() any {
		return map[string]any{
			"assembled": map[string]any{"hits": 3, "misses": 1, "loads": 1, "hit_ratio": 0.75},
			"base":      map[string]any{"hits": 2, "misses": 1, "loads": 1, "patches": 1, "rebuilds": 0},
		}
	}
	enabledDeps.ResetCacheStats = func() { resetCalls++ }
	staticRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(staticRoot, "index.html"), []byte("<!doctype html><title>spa</title>"), 0o600); err != nil {
		t.Fatal(err)
	}
	enabledDeps.FrontendStaticPath = staticRoot
	enabled := New(enabledDeps)

	req := httptest.NewRequest(http.MethodGet, "/debug/cache/stats", nil)
	rec := httptest.NewRecorder()
	enabled.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("enabled cache stats status=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<!doctype html>") {
		t.Fatalf("cache stats returned SPA HTML: %s", rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["enabled"] != true {
		t.Fatalf("body=%#v", body)
	}
	// Response may nest under "stats" or "layers" depending on handler shape.
	statsObj, _ := body["stats"].(map[string]any)
	if statsObj == nil {
		statsObj, _ = body["layers"].(map[string]any)
	}
	if statsObj == nil || (statsObj["assembled"] == nil && statsObj["Assembled"] == nil) {
		t.Fatalf("missing stats payload: %#v", body)
	}

	resetReq := httptest.NewRequest(http.MethodPost, "/debug/cache/stats/reset", nil)
	resetRec := httptest.NewRecorder()
	enabled.ServeHTTP(resetRec, resetReq)
	if resetRec.Code != http.StatusOK || resetCalls != 1 {
		t.Fatalf("reset status=%d calls=%d body=%s", resetRec.Code, resetCalls, resetRec.Body.String())
	}

	// Mutating GET must not be registered (prefetch-safe).
	getReset := httptest.NewRequest(http.MethodGet, "/debug/cache/stats/reset", nil)
	getRec := httptest.NewRecorder()
	enabled.ServeHTTP(getRec, getReset)
	if getRec.Code == http.StatusOK && resetCalls > 1 {
		t.Fatalf("GET reset mutated state: status=%d calls=%d", getRec.Code, resetCalls)
	}
	if resetCalls != 1 {
		t.Fatalf("GET reset should not call reset handler: calls=%d", resetCalls)
	}
}
