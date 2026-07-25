package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

// countingListBasesRepo counts full-pool base list calls.
type countingListBasesRepo struct {
	repository.AccountRepository
	listBaseCalls atomic.Int64
}

func (r *countingListBasesRepo) ListRoutingAccountBases(ctx context.Context, provider account.Provider, quotaMode string) ([]account.RoutingAccountBase, error) {
	r.listBaseCalls.Add(1)
	return r.AccountRepository.(interface {
		ListRoutingAccountBases(context.Context, account.Provider, string) ([]account.RoutingAccountBase, error)
	}).ListRoutingAccountBases(ctx, provider, quotaMode)
}

func (r *countingListBasesRepo) ListRoutingAccountOverlays(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel string) (account.RoutingOverlaySnapshot, error) {
	return r.AccountRepository.(interface {
		ListRoutingAccountOverlays(context.Context, account.Provider, uint64, string) (account.RoutingOverlaySnapshot, error)
	}).ListRoutingAccountOverlays(ctx, provider, modelRouteID, upstreamModel)
}

func (r *countingListBasesRepo) GetRoutingAccountBase(ctx context.Context, id uint64, quotaMode string) (account.RoutingAccountBase, error) {
	return r.AccountRepository.(repository.RoutingAccountLookup).GetRoutingAccountBase(ctx, id, quotaMode)
}

func (r *countingListBasesRepo) GetRoutingCandidate(ctx context.Context, id uint64, modelRouteID uint64, upstreamModel, quotaMode string) (account.RoutingCandidate, error) {
	return r.AccountRepository.(repository.RoutingAccountLookup).GetRoutingCandidate(ctx, id, modelRouteID, upstreamModel, quotaMode)
}

func wrapCountingBases(inner *relational.AccountRepository) *countingListBasesRepo {
	return &countingListBasesRepo{AccountRepository: inner}
}

func TestAccountScopedInvalidationDoesNotForceFullBaseReload(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "patch-base.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	repo := wrapCountingBases(inner)
	primary, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backup, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "backup", SourceKey: "backup",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	inner.SetInvalidationObserver(func(_ context.Context, event repository.InvalidationEvent) {
		selector.ApplyInvalidation(event)
	})

	// Warm L1/L3.
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	loadsAfterWarm := repo.listBaseCalls.Load()
	if loadsAfterWarm < 1 {
		t.Fatal("expected at least one full base list to warm cache")
	}
	stats := selector.CacheStats()
	if stats.Base.Hits+stats.Assembled.Hits < 1 && stats.Base.Loads < 1 {
		t.Fatalf("expected cache activity after warm: %#v", stats)
	}

	// Single-account cooldown via MarkFailure → UpdateHealth → invalidation with AccountID.
	selector.MarkFailure(ctx, primary, 429, time.Hour)

	// Next load must not require another full-pool ListRoutingAccountBases.
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != backup.ID {
		t.Fatalf("after cooldown selected %d want backup %d", lease.Credential.ID, backup.ID)
	}
	lease.Release()

	if repo.listBaseCalls.Load() != loadsAfterWarm {
		t.Fatalf("single-account invalidation forced full base reload: warm=%d now=%d", loadsAfterWarm, repo.listBaseCalls.Load())
	}
	stats = selector.CacheStats()
	if stats.Base.Patches < 1 {
		t.Fatalf("expected base patch counter >= 1, got %#v", stats.Base)
	}
	if stats.Invalidation.BulkRebuilds != 0 {
		t.Fatalf("unexpected bulk rebuilds: %#v", stats.Invalidation)
	}
}

func TestTokenOnlyInvalidationDoesNotBumpBaseOrClearPool(t *testing.T) {
	repo := newLayeredRepositoryFixture()
	selector := NewSelector(repo, nil, nil, nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	baseCalls, _ := repo.callCounts("model-a")
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountCredentialChanged, Provider: account.ProviderBuild, AccountID: 1,
	})
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	baseCalls2, _ := repo.callCounts("model-a")
	if baseCalls2 != baseCalls {
		t.Fatalf("token-only invalidation reloaded base: %d -> %d", baseCalls, baseCalls2)
	}
	if selector.CacheStats().Invalidation.TokenOnlySkipped < 1 {
		t.Fatal("expected token_only_skipped counter")
	}
}

func TestAssembledMissUsesWarmBaseWithoutExtraFullList(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "assemble-mem.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	repo := wrapCountingBases(inner)
	if _, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "a", SourceKey: "a",
		EncryptedAccessToken: "t", AuthStatus: account.AuthStatusActive, Enabled: true, MaxConcurrent: 2,
	}); err != nil {
		t.Fatal(err)
	}
	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)

	// Warm base via first model.
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model-a", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	loads := repo.listBaseCalls.Load()
	// Invalidate only assembled (capability/overlay style) — base should stay warm.
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountCapabilityChanged, Provider: account.ProviderBuild,
	})
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model-b", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if repo.listBaseCalls.Load() != loads {
		t.Fatalf("assembled miss reloaded full base pool: %d -> %d", loads, repo.listBaseCalls.Load())
	}
	stats := selector.CacheStats()
	if stats.Base.Hits < 1 {
		t.Fatalf("expected base hits after warm reuse: %#v", stats)
	}
}

func TestBulkInvalidationStillRebuildsBase(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "bulk-rebuild.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	repo := wrapCountingBases(inner)
	if _, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "a", SourceKey: "a",
		EncryptedAccessToken: "t", AuthStatus: account.AuthStatusActive, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "m", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	loads := repo.listBaseCalls.Load()
	// Bulk base event without AccountID.
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountStateChanged, Provider: account.ProviderBuild,
	})
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "m", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if repo.listBaseCalls.Load() <= loads {
		t.Fatalf("bulk invalidation should force base reload: %d -> %d", loads, repo.listBaseCalls.Load())
	}
	if selector.CacheStats().Invalidation.BulkRebuilds < 1 {
		t.Fatal("expected bulk_rebuilds counter")
	}
}

func TestCacheStatsReflectHitsAndMisses(t *testing.T) {
	repo := newLayeredRepositoryFixture()
	selector := NewSelector(repo, nil, nil, nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	selector.ResetCacheStats()
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	stats := selector.CacheStats()
	if stats.Assembled.Hits < 1 {
		t.Fatalf("expected assembled hit on second load: %#v", stats.Assembled)
	}
	if stats.Assembled.Misses < 1 {
		t.Fatalf("expected assembled miss on first load: %#v", stats.Assembled)
	}
	if stats.Assembled.HitRatio == nil || *stats.Assembled.HitRatio <= 0 {
		t.Fatalf("hit_ratio missing: %#v", stats.Assembled)
	}
	selector.ResetCacheStats()
	if selector.CacheStats().Assembled.Hits != 0 {
		t.Fatal("reset did not clear counters")
	}
}

// failingGetBasesRepo wraps a counting repo and forces GetRoutingAccountBase errors.
type failingGetBasesRepo struct {
	*countingListBasesRepo
	failGet atomic.Bool
	getCalls atomic.Int64
}

func (r *failingGetBasesRepo) GetRoutingAccountBase(ctx context.Context, id uint64, quotaMode string) (account.RoutingAccountBase, error) {
	r.getCalls.Add(1)
	if r.failGet.Load() {
		return account.RoutingAccountBase{}, errors.New("simulated point-load db blip")
	}
	return r.countingListBasesRepo.GetRoutingAccountBase(ctx, id, quotaMode)
}

func TestPointLoadErrorFailsOpenToFullBaseReload(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "point-fail.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	counted := wrapCountingBases(inner)
	repo := &failingGetBasesRepo{countingListBasesRepo: counted}
	primary, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "backup", SourceKey: "backup",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 1,
	}); err != nil {
		t.Fatal(err)
	}

	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	loadsAfterWarm := repo.listBaseCalls.Load()
	if loadsAfterWarm < 1 {
		t.Fatal("expected warm full list")
	}

	repo.failGet.Store(true)
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountStateChanged, Provider: account.ProviderBuild, AccountID: primary.ID,
	})
	if selector.CacheStats().Invalidation.BulkRebuilds < 1 {
		t.Fatalf("expected bulk rebuild on point-load error: %#v", selector.CacheStats().Invalidation)
	}

	// Next load must full-list again — not serve a truncated warm pool for soft TTL.
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if repo.listBaseCalls.Load() <= loadsAfterWarm {
		t.Fatalf("point-load error should force full base reload: warm=%d now=%d", loadsAfterWarm, repo.listBaseCalls.Load())
	}
}

// noLookupRoutingRepo embeds AccountRepository (interface) and re-exposes layer lists, but does
// NOT implement RoutingAccountLookup. Point-get methods on the concrete sqlite repo are not
// promoted through the interface embed, so patchBaseAccount must fail open.
type noLookupRoutingRepo struct {
	repository.AccountRepository
	listBaseCalls atomic.Int64
}

func (r *noLookupRoutingRepo) ListRoutingAccountBases(ctx context.Context, provider account.Provider, quotaMode string) ([]account.RoutingAccountBase, error) {
	r.listBaseCalls.Add(1)
	return r.AccountRepository.(interface {
		ListRoutingAccountBases(context.Context, account.Provider, string) ([]account.RoutingAccountBase, error)
	}).ListRoutingAccountBases(ctx, provider, quotaMode)
}

func (r *noLookupRoutingRepo) ListRoutingAccountOverlays(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel string) (account.RoutingOverlaySnapshot, error) {
	return r.AccountRepository.(interface {
		ListRoutingAccountOverlays(context.Context, account.Provider, uint64, string) (account.RoutingOverlaySnapshot, error)
	}).ListRoutingAccountOverlays(ctx, provider, modelRouteID, upstreamModel)
}

func TestNoLookupAccountInvalidationFailsOpenToFullReload(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "no-lookup.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	repo := &noLookupRoutingRepo{AccountRepository: inner}
	if _, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "a", SourceKey: "a",
		EncryptedAccessToken: "t", AuthStatus: account.AuthStatusActive, Enabled: true, MaxConcurrent: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "b", SourceKey: "b",
		EncryptedAccessToken: "t", AuthStatus: account.AuthStatusActive, Enabled: true, MaxConcurrent: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Confirm the wrapper is not a RoutingAccountLookup (regression guard).
	if _, ok := any(repo).(repository.RoutingAccountLookup); ok {
		t.Fatal("noLookupRoutingRepo must not implement RoutingAccountLookup")
	}

	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", now); err != nil {
		t.Fatal(err)
	}
	warm := repo.listBaseCalls.Load()
	if warm < 1 {
		t.Fatal("expected warm list")
	}

	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountStateChanged, Provider: account.ProviderBuild, AccountID: 1,
	})
	if selector.CacheStats().Invalidation.BulkRebuilds < 1 {
		t.Fatalf("no-lookup account event should bulk rebuild: %#v", selector.CacheStats().Invalidation)
	}
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", now); err != nil {
		t.Fatal(err)
	}
	if repo.listBaseCalls.Load() <= warm {
		t.Fatalf("expected full reload after no-lookup account invalidation: warm=%d now=%d", warm, repo.listBaseCalls.Load())
	}
	values, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 candidates after fail-open reload, got %d", len(values))
	}
}

func TestPointLoadNotFoundRemovesOnlyThatAccount(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "not-found-patch.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	repo := wrapCountingBases(inner)
	primary, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backup, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "backup", SourceKey: "backup",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	warm := repo.listBaseCalls.Load()

	// Delete primary so point Get returns ErrNotFound → remove from L1, keep backup.
	if err := inner.Delete(ctx, primary.ID); err != nil {
		t.Fatal(err)
	}
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountStateChanged, Provider: account.ProviderBuild, AccountID: primary.ID,
	})
	if selector.CacheStats().Invalidation.BulkRebuilds != 0 {
		t.Fatalf("not-found should patch-remove, not bulk rebuild: %#v", selector.CacheStats().Invalidation)
	}
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != backup.ID {
		t.Fatalf("selected %d want backup %d", lease.Credential.ID, backup.ID)
	}
	lease.Release()
	if repo.listBaseCalls.Load() != warm {
		t.Fatalf("not-found removal should not full-list: warm=%d now=%d", warm, repo.listBaseCalls.Load())
	}
}
