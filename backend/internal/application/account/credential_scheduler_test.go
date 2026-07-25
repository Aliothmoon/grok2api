package account

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"

	sqlite "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestRefreshDueCredentialsDoesNotReconcileEveryTick(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	service, credential, adapter := newCredentialRefreshTestService(t, now)
	// Seed a non-NULL due so ListDue works without Backfill.
	due := now.Add(-time.Minute)
	credential.RefreshDueAt = &due
	if _, err := service.accounts.Update(ctx, credential); err != nil {
		t.Fatal(err)
	}

	// First due pass may throttle-reconcile once (lastReconcile zero).
	if err := service.refreshDueCredentials(ctx); err != nil {
		t.Fatal(err)
	}
	first := service.CredentialReconcileCallCount()
	if first != 1 {
		t.Fatalf("first reconcile count = %d want 1", first)
	}
	if adapter.refreshCount.Load() != 1 {
		t.Fatalf("refresh count = %d want 1", adapter.refreshCount.Load())
	}

	// Advance past process-local forced-refresh cooldown so a second OAuth can run.
	// Reconcile throttle (15m) is longer; advancing only ~31s must NOT re-run Backfill.
	now2 := service.now().Add(forcedRefreshMinInterval + time.Second)
	service.now = func() time.Time { return now2 }

	// Force another due row without waiting for real token expiry.
	due2 := now2.Add(-time.Second)
	updated, err := service.accounts.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	updated.RefreshDueAt = &due2
	updated.EncryptedAccessToken = "access-stale"
	updated.EncryptedRefreshToken = "refresh-0"
	if _, err := service.accounts.Update(ctx, updated); err != nil {
		t.Fatal(err)
	}

	if err := service.refreshDueCredentials(ctx); err != nil {
		t.Fatal(err)
	}
	second := service.CredentialReconcileCallCount()
	if second != first {
		t.Fatalf("second due tick re-ran reconcile: first=%d second=%d", first, second)
	}
	if adapter.refreshCount.Load() != 2 {
		t.Fatalf("refresh count = %d want 2 (due still processed without reconcile)", adapter.refreshCount.Load())
	}
}

func TestForcedReconcileStillBackfillsMissingDue(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dbPath := filepath.Join(t.TempDir(), "reconcile-force.db")
	database, err := relational.OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	repo := relational.NewAccountRepository(database)
	// Create via Upsert then clear due to simulate upgrade-era row.
	cred, _, err := repo.UpsertByIdentity(ctx, accountdomain.Credential{
		Provider: accountdomain.ProviderBuild, Name: "legacy", SourceKey: "legacy",
		EncryptedAccessToken: "a", EncryptedRefreshToken: "r", ExpiresAt: now.Add(time.Hour),
		Enabled: true, AuthStatus: accountdomain.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Force NULL refresh_due_at (upgrade-era) so Backfill has real work.
	// Database.db is unexported; open a second GORM handle on the same SQLite file.
	side, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, dbErr := side.DB(); dbErr == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	if err := side.WithContext(ctx).Exec(
		"UPDATE account_credentials SET refresh_due_at = NULL WHERE account_id = ?", cred.ID,
	).Error; err != nil {
		t.Fatal(err)
	}
	before, err := repo.Get(ctx, cred.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.RefreshDueAt != nil {
		t.Fatal("setup failed: expected NULL refresh_due_at before reconcile")
	}

	adapter := &credentialRefreshAdapter{}
	service := NewService(repo, nil, nil, nil, provider.NewRegistry(adapter), nil, nil)
	service.now = func() time.Time { return now }
	n, err := service.ReconcileCredentialSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("forced reconcile backfilled %d rows, want >= 1", n)
	}
	if service.CredentialReconcileCallCount() < 1 {
		t.Fatal("forced reconcile did not record a call")
	}
	got, err := repo.Get(ctx, cred.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RefreshDueAt == nil {
		t.Fatal("forced reconcile left refresh_due_at NULL")
	}
}

func TestListDueWorksWithoutReconcileWhenDuePresent(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	service, credential, _ := newCredentialRefreshTestService(t, now)
	due := now.Add(-time.Minute)
	credential.RefreshDueAt = &due
	if _, err := service.accounts.Update(ctx, credential); err != nil {
		t.Fatal(err)
	}
	// Mark reconcile as recently done so throttled path skips Backfill.
	service.credentialReconcileMu.Lock()
	service.lastCredentialReconcileAt = now
	service.credentialReconcileMu.Unlock()
	before := service.CredentialReconcileCallCount()
	if err := service.refreshDueCredentials(ctx); err != nil {
		t.Fatal(err)
	}
	if service.CredentialReconcileCallCount() != before {
		t.Fatalf("reconcile ran despite throttle: before=%d after=%d", before, service.CredentialReconcileCallCount())
	}
}
