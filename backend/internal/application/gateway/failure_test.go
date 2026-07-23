package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

func TestTransportUpstreamFailureFingerprintIsAccountScopedOnlyForProxyPool(t *testing.T) {
	networkErr := errors.New(`Post "https://cli-chat-proxy.grok.com/v1/responses": http2: timeout awaiting response headers`)

	sharedA := newTransportUpstreamFailure(networkErr, 5471, "account-a", false)
	sharedB := newTransportUpstreamFailure(networkErr, 5472, "account-b", false)
	if sharedA.Fingerprint != "upstream_network_error" || sharedB.Fingerprint != "upstream_network_error" {
		t.Fatalf("non-pool fingerprints = %q / %q", sharedA.Fingerprint, sharedB.Fingerprint)
	}
	if sharedA.AccountScoped || sharedB.AccountScoped {
		t.Fatal("非代理池传输失败不应标记 AccountScoped")
	}

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

type responseHeaderTimeoutTestError struct{}

func (responseHeaderTimeoutTestError) Error() string {
	return "http2: timeout awaiting response headers"
}
func (responseHeaderTimeoutTestError) Timeout() bool   { return true }
func (responseHeaderTimeoutTestError) Temporary() bool { return true }

func TestTransportUpstreamFailureClassifiesResponseHeaderTimeout(t *testing.T) {
	failure := newTransportUpstreamFailure(responseHeaderTimeoutTestError{}, 42, "build", false)
	if failure.HTTPStatus != http.StatusGatewayTimeout || failure.Code != "upstream_header_timeout" || failure.PublicMessage != "等待上游响应头超时" || failure.AuditCode() != "upstream_header_timeout" {
		t.Fatalf("failure = %#v", failure)
	}
	if stage := transportStage(responseHeaderTimeoutTestError{}); stage != "response_header_timeout" {
		t.Fatalf("stage = %q", stage)
	}
	if isRetryableTransportFailure(accountdomain.ProviderBuild, responseHeaderTimeoutTestError{}) {
		t.Fatal("a Build response-header timeout must not switch accounts")
	}
	if !isRetryableTransportFailure(accountdomain.ProviderWeb, responseHeaderTimeoutTestError{}) {
		t.Fatal("the Build-specific retry veto must not change Web failover")
	}
	if !isRetryableTransportFailure(accountdomain.ProviderBuild, errors.New("connection reset by peer")) {
		t.Fatal("ordinary pre-response transport failures must retain failover behavior")
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
		accountBlocked         bool
		upstreamCode           string
	}{
		{
			name: "blocked account", body: `{"code":"unauthorized:blocked-user","error":"User is blocked"}`,
			accountScoped: true, accountBlocked: true, upstreamCode: "unauthorized:blocked-user",
		},
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
			if failure.HTTPStatus != http.StatusForbidden || failure.Code != "upstream_forbidden" || failure.AccountScoped != test.accountScoped || failure.AccountBlocked != test.accountBlocked || failure.PermanentAccountDenial != test.permanentAccountDenial || failure.QuotaExhausted != test.quotaExhausted || failure.FreeQuotaExhausted != test.freeQuotaExhausted || failure.ModelQuotaExhausted != test.modelQuotaExhausted || failure.UpstreamCode != test.upstreamCode {
				t.Fatalf("failure = %#v", failure)
			}
			if test.upstreamCode == "permission-denied" && (failure.ClientCredentialErrorCode() != "permission-denied" || failure.AuditCode() != "upstream_forbidden_permission_denied") {
				t.Fatalf("public=%q audit=%q", failure.ClientCredentialErrorCode(), failure.AuditCode())
			}
		})
	}
}

func TestHTTPUpstreamFailureLeavesPaymentRecoveryKindToBilling(t *testing.T) {
	failure := newHTTPUpstreamFailure(http.StatusPaymentRequired, []byte(`{
		"code":"personal-team-blocked:spending-limit",
		"error":"You have run out of credits"
	}`), 42, "build")
	if !failure.AccountScoped || !failure.QuotaExhausted || failure.FreeQuotaExhausted || failure.UpstreamCode != "personal-team-blocked:spending-limit" {
		t.Fatalf("failure = %#v", failure)
	}
}

func TestRetryableResponseHonorsUpstreamRetryVeto(t *testing.T) {
	response := &provider.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     http.Header{"X-Should-Retry": {"false"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":"invalid request history"}`)),
	}
	if isRetryableResponse(response, accountdomain.ProviderBuild) {
		t.Fatal("x-should-retry:false 必须禁止换账号重试")
	}
	response.Header.Set("X-Should-Retry", "true")
	if !isRetryableResponse(response, accountdomain.ProviderBuild) {
		t.Fatal("x-should-retry:true 不应覆盖现有状态码重试策略")
	}
	response.Header.Set("X-Should-Retry", "unknown")
	if !isRetryableResponse(response, accountdomain.ProviderBuild) {
		t.Fatal("未知 x-should-retry 值应按未提供处理")
	}
}

func TestPaymentRequiredAlwaysRetriesDespiteUpstreamVeto(t *testing.T) {
	response := &provider.Response{
		StatusCode: http.StatusPaymentRequired,
		Header:     http.Header{"X-Should-Retry": {"false"}},
		Body:       io.NopCloser(strings.NewReader(`{"code":"personal-team-blocked:spending-limit","error":"You have run out of credits"}`)),
	}
	if !isRetryableResponse(response, accountdomain.ProviderBuild) {
		t.Fatal("402 spending-limit must force account rotation even when X-Should-Retry is false")
	}
	if isRetryableResponse(response, accountdomain.ProviderWeb) {
		t.Fatal("non-Build 402 must continue honoring X-Should-Retry:false")
	}
}

func TestBuildForbiddenAlwaysEntersAccountFailureHandling(t *testing.T) {
	response := &provider.Response{
		StatusCode: http.StatusForbidden,
		Header:     http.Header{"X-Should-Retry": {"false"}},
		Body:       io.NopCloser(strings.NewReader(`{"code":"permission-denied"}`)),
	}
	if !isRetryableResponse(response, accountdomain.ProviderBuild) {
		t.Fatal("Build 403 must enter account failure handling even when X-Should-Retry is false")
	}
	if isRetryableResponse(response, accountdomain.ProviderWeb) {
		t.Fatal("non-Build 403 must continue honoring X-Should-Retry:false")
	}
}

func TestBuildForbiddenReauthPolicyMatchesExactErrorCodes(t *testing.T) {
	service := &Service{}
	service.UpdateBuildForbiddenReauthPolicy(true, []string{"permission-denied", "team-access-denied"})

	for _, code := range []string{"permission-denied", "TEAM-ACCESS-DENIED"} {
		failure := &UpstreamFailure{HTTPStatus: http.StatusForbidden, UpstreamCode: code}
		if !service.shouldInvalidateBuildForbidden(failure) {
			t.Fatalf("configured code %q did not match", code)
		}
	}
	for _, failure := range []*UpstreamFailure{
		{HTTPStatus: http.StatusForbidden, UpstreamCode: "permission_denied"},
		{HTTPStatus: http.StatusForbidden, UpstreamCode: "unconfigured-denial"},
		{HTTPStatus: http.StatusUnauthorized, UpstreamCode: "permission-denied"},
		{HTTPStatus: http.StatusInternalServerError, UpstreamCode: "permission-denied"},
	} {
		if service.shouldInvalidateBuildForbidden(failure) {
			t.Fatalf("unconfigured or ineligible failure matched: %#v", failure)
		}
	}

	service.UpdateBuildForbiddenReauthPolicy(false, []string{"permission-denied"})
	if service.shouldInvalidateBuildForbidden(&UpstreamFailure{HTTPStatus: http.StatusForbidden, UpstreamCode: "permission-denied"}) {
		t.Fatal("disabled policy matched an error code")
	}
}
