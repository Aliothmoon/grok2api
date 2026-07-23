package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

func TestTransportUpstreamFailureFingerprintIsAccountScopedOnlyForProxyPool(t *testing.T) {
	networkErr := errors.New(`Post "https://cli-chat-proxy.grok.com/v1/responses": http2: timeout awaiting response headers`)

	// Shared single proxy / direct: keep coarse fingerprint so two failures still short-circuit.
	sharedA := newTransportUpstreamFailure(networkErr, 5471, "account-a", false)
	sharedB := newTransportUpstreamFailure(networkErr, 5472, "account-b", false)
	if sharedA.Fingerprint != "upstream_network_error" || sharedB.Fingerprint != "upstream_network_error" {
		t.Fatalf("non-pool fingerprints = %q / %q", sharedA.Fingerprint, sharedB.Fingerprint)
	}
	if sharedA.AccountScoped || sharedB.AccountScoped {
		t.Fatal("非代理池传输失败不应标记 AccountScoped")
	}

	// Proxy pool / sticky account template: each account is a different egress path.
	first := newTransportUpstreamFailure(networkErr, 5471, "account-a", true)
	second := newTransportUpstreamFailure(networkErr, 5472, "account-b", true)
	if first.Code != "upstream_network_error" || second.Code != "upstream_network_error" {
		t.Fatalf("code = %q / %q", first.Code, second.Code)
	}
	if first.Fingerprint != "upstream_network_error:account:5471" {
		t.Fatalf("first fingerprint = %q", first.Fingerprint)
	}
	if second.Fingerprint != "upstream_network_error:account:5472" {
		t.Fatalf("second fingerprint = %q", second.Fingerprint)
	}
	if first.Fingerprint == second.Fingerprint {
		t.Fatal("代理池下不同账号的传输失败指纹必须区分")
	}
	if !first.AccountScoped || !second.AccountScoped {
		t.Fatal("代理池下带账号的传输失败应标记 AccountScoped")
	}

	deadline := newTransportUpstreamFailure(context.DeadlineExceeded, 99, "account-c", true)
	if deadline.Code != "upstream_timeout" || deadline.Fingerprint != "upstream_timeout:account:99" {
		t.Fatalf("deadline failure = %#v", deadline)
	}

	anonymous := newTransportUpstreamFailure(networkErr, 0, "", true)
	if anonymous.Fingerprint != "upstream_network_error" || anonymous.AccountScoped {
		t.Fatalf("代理池但无账号 = %#v", anonymous)
	}
}

func TestHTTPUpstreamFailureClassifiesBuildForbiddenBodies(t *testing.T) {
	tests := []struct {
		name                   string
		body                   string
		accountScoped          bool
		permanentAccountDenial bool
		quotaExhausted         bool
		freeQuotaExhausted     bool
		modelQuotaExhausted    bool
		upstreamCode           string
	}{
		{
			name: "top-level permanent chat denial", body: `{"status_code":403,"code":"permission-denied","error":"Access to the chat endpoint is denied. Please update the permissions."}`,
			accountScoped: true, permanentAccountDenial: true, upstreamCode: "permission-denied",
		},
		{
			name: "spending limit", body: `{"code":"personal-team-blocked:spending-limit","error":"quota exhausted"}`,
			accountScoped: true, quotaExhausted: true, upstreamCode: "personal-team-blocked:spending-limit",
		},
		{
			name: "unknown policy rejection", body: `{"error":"upstream policy rejected request"}`,
		},
		{
			name: "free model quota", body: `{"error":"You've used all the included free usage for model grok-build"}`,
			accountScoped: true, quotaExhausted: true, freeQuotaExhausted: true, modelQuotaExhausted: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := newHTTPUpstreamFailure(http.StatusForbidden, []byte(test.body), 42, "build")
			if failure.HTTPStatus != http.StatusForbidden || failure.Code != "upstream_forbidden" || failure.AccountScoped != test.accountScoped || failure.PermanentAccountDenial != test.permanentAccountDenial || failure.QuotaExhausted != test.quotaExhausted || failure.FreeQuotaExhausted != test.freeQuotaExhausted || failure.ModelQuotaExhausted != test.modelQuotaExhausted || failure.UpstreamCode != test.upstreamCode {
				t.Fatalf("failure = %#v", failure)
			}
			if test.upstreamCode == "permission-denied" && (failure.ClientCredentialErrorCode() != "permission-denied" || failure.AuditCode() != "upstream_forbidden_permission_denied") {
				t.Fatalf("public=%q audit=%q", failure.ClientCredentialErrorCode(), failure.AuditCode())
			}
		})
	}
}

func TestRetryableResponseHonorsUpstreamRetryVeto(t *testing.T) {
	response := &provider.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     http.Header{"X-Should-Retry": {"false"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":"invalid request history"}`)),
	}
	if isRetryableResponse(response) {
		t.Fatal("x-should-retry:false 必须禁止换账号重试")
	}
	response.StatusCode = http.StatusPaymentRequired
	if !isRetryableResponse(response) {
		t.Fatal("账号级 402 必须忽略上游的原账号重试 veto，允许跨账号故障转移")
	}
	response.StatusCode = http.StatusInternalServerError
	response.Header.Set("X-Should-Retry", "true")
	if !isRetryableResponse(response) {
		t.Fatal("x-should-retry:true 不应覆盖现有状态码重试策略")
	}
	response.Header.Set("X-Should-Retry", "unknown")
	if !isRetryableResponse(response) {
		t.Fatal("未知 x-should-retry 值应按未提供处理")
	}
}
