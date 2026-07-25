package relational

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

func TestListRoutingAccountBasesExcludesEncryptedTokens(t *testing.T) {
	ctx := context.Background()
	database, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "routing-projection.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := NewAccountRepository(database)
	created, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "secret-account", SourceKey: "secret-account",
		EncryptedAccessToken: "access-secret", EncryptedRefreshToken: "refresh-secret",
		EncryptedCloudflareCookie: "cf-secret", AuthStatus: account.AuthStatusActive, Enabled: true,
		Priority: 10, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	full, err := accounts.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !account.HasRoutingSecrets(full) {
		t.Fatalf("Get should retain secrets: %#v", full)
	}

	bases, err := accounts.ListRoutingAccountBases(ctx, account.ProviderBuild, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) != 1 {
		t.Fatalf("bases = %#v", bases)
	}
	if account.HasRoutingSecrets(bases[0].Credential) {
		t.Fatalf("routing base leaked secrets: %#v", bases[0].Credential)
	}
	if bases[0].Credential.ID != created.ID || bases[0].Credential.Priority != 10 {
		t.Fatalf("routing projection missing identity fields: %#v", bases[0].Credential)
	}
	if bases[0].Credential.AuthType != account.AuthTypeOAuth {
		t.Fatalf("routing projection missing auth type: %#v", bases[0].Credential)
	}

	candidates, err := accounts.ListRoutingCandidates(ctx, account.ProviderBuild, 0, "model-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || account.HasRoutingSecrets(candidates[0].Credential) {
		t.Fatalf("routing candidates leaked secrets: %#v", candidates)
	}
}

func TestGetRoutingCandidateDoesNotScanFullPool(t *testing.T) {
	ctx := context.Background()
	database, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "routing-point.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := NewAccountRepository(database)
	models := NewModelRepository(database)
	target, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "target", SourceKey: "target",
		EncryptedAccessToken: "access", EncryptedRefreshToken: "refresh",
		AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
			Provider: account.ProviderBuild, Name: "other", SourceKey: "other-" + string(rune('a'+i)),
			EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	if err := models.ReplaceAccountCapabilities(ctx, target.ID, []string{"model-a"}, now); err != nil {
		t.Fatal(err)
	}
	if err := accounts.SaveQuotaRecovery(ctx, account.QuotaRecovery{
		AccountID: target.ID, Kind: account.QuotaRecoveryKindFree, Status: account.QuotaRecoveryStatusExhausted,
		NextProbeAt: &now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	candidate, err := accounts.GetRoutingCandidate(ctx, target.ID, 0, "model-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Credential.ID != target.ID {
		t.Fatalf("candidate id = %d", candidate.Credential.ID)
	}
	if account.HasRoutingSecrets(candidate.Credential) {
		t.Fatalf("point lookup leaked secrets: %#v", candidate.Credential)
	}
	if !candidate.SupportsModel || candidate.QuotaRecovery == nil {
		t.Fatalf("candidate missing attachments: %#v", candidate)
	}

	missing, err := accounts.GetRoutingCandidate(ctx, 999999, 0, "model-a", "")
	if !errorsIsNotFound(err) {
		t.Fatalf("missing candidate = %#v err=%v", missing, err)
	}
}

func TestGetRoutingCandidateApplySharedSuperBuildModel(t *testing.T) {
	ctx := context.Background()
	database, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "routing-super-shared.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := NewAccountRepository(database)
	models := NewModelRepository(database)
	// Super-entitled pinned account with no capability row of its own.
	pinned, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "pinned-super", SourceKey: "pinned-super",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true,
		BuildSuperEntitled: true, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Another Super account that does support the model.
	other, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "other-super", SourceKey: "other-super",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true,
		BuildSuperEntitled: true, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := models.ReplaceAccountCapabilities(ctx, other.ID, []string{"grok-super-shared"}, now); err != nil {
		t.Fatal(err)
	}

	candidate, err := accounts.GetRoutingCandidate(ctx, pinned.ID, 0, "grok-super-shared", "")
	if err != nil {
		t.Fatal(err)
	}
	if !candidate.SupportsModel || !candidate.ModelCapabilityKnown {
		t.Fatalf("Super shared model not applied to pinned candidate: %#v", candidate)
	}
	if account.HasRoutingSecrets(candidate.Credential) {
		t.Fatalf("pinned candidate leaked secrets: %#v", candidate.Credential)
	}
}

func TestGetRoutingCandidateRespectsRouteBinding(t *testing.T) {
	ctx := context.Background()
	database, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "routing-bound.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := NewAccountRepository(database)
	models := NewModelRepository(database)
	bound, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "bound", SourceKey: "bound",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	unbound, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "unbound", SourceKey: "unbound",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	route, err := models.Create(ctx, model.Route{
		PublicID: "public-a", Provider: account.ProviderBuild, UpstreamModel: "model-a",
		Capability: model.CapabilityResponses, Enabled: true,
	}, []uint64{bound.ID})
	if err != nil {
		t.Fatal(err)
	}
	okCandidate, err := accounts.GetRoutingCandidate(ctx, bound.ID, route.ID, "model-a", "")
	if err != nil || okCandidate.Credential.ID != bound.ID || !okCandidate.SupportsModel {
		t.Fatalf("bound candidate = %#v err=%v", okCandidate, err)
	}
	if _, err := accounts.GetRoutingCandidate(ctx, unbound.ID, route.ID, "model-a", ""); !errorsIsNotFound(err) {
		t.Fatalf("unbound account should be missing under route binding, err=%v", err)
	}
}

func errorsIsNotFound(err error) bool {
	return err != nil && (err == repository.ErrNotFound || err.Error() == repository.ErrNotFound.Error())
}
