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

func TestSparseAccountConcurrencyEquivalenceOnPlanPath(t *testing.T) {
	ctx := context.Background()
	limiter := memory.NewConcurrencyLimiter()
	// Mixed occupancy: account 1 has 2 leases, 2 has 0, 3 has 1.
	r1, ok, err := limiter.Acquire(ctx, repository.AccountConcurrencyKey(1), 3)
	if err != nil || !ok {
		t.Fatal(err)
	}
	defer r1()
	r1b, ok, err := limiter.Acquire(ctx, repository.AccountConcurrencyKey(1), 3)
	if err != nil || !ok {
		t.Fatal(err)
	}
	defer r1b()
	r3, ok, err := limiter.Acquire(ctx, repository.AccountConcurrencyKey(3), 2)
	if err != nil || !ok {
		t.Fatal(err)
	}
	defer r3()

	ids := []uint64{1, 2, 3}
	keys := []string{
		repository.AccountConcurrencyKey(1),
		repository.AccountConcurrencyKey(2),
		repository.AccountConcurrencyKey(3),
	}
	dense, err := limiter.CurrentMany(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	sparse, err := limiter.CurrentManyAccountIDs(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if sparse[id] != dense[keys[i]] {
			t.Fatalf("id %d: sparse=%d dense=%d", id, sparse[id], dense[keys[i]])
		}
	}
	if len(sparse) != 2 {
		t.Fatalf("sparse should only store nonzero counts, got %#v", sparse)
	}

	// Drive real planCandidateIndexes (shipped path) and assert ordering uses sparse inFlight.
	values := []account.RoutingCandidate{
		{Credential: account.Credential{ID: 1, Provider: account.ProviderBuild, AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 3}},
		{Credential: account.Credential{ID: 2, Provider: account.ProviderBuild, AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 3}},
		{Credential: account.Credential{ID: 3, Provider: account.ProviderBuild, AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 3}},
	}
	selector := NewSelector(nil, limiter, nil, nil, time.Hour, time.Second, time.Minute)
	plan, err := selector.planCandidateIndexes(ctx, values, nil, time.Now().UTC(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Lower inFlight first: id 2 (0) then id 3 (1) then id 1 (2), priority equal.
	first, ok := plan.Next()
	if !ok || first.Credential.ID != 2 {
		t.Fatalf("first planned = %#v, want id 2 (zero in-flight)", first)
	}
	second, ok := plan.Next()
	if !ok || second.Credential.ID != 3 {
		t.Fatalf("second planned = %#v, want id 3", second)
	}
	third, ok := plan.Next()
	if !ok || third.Credential.ID != 1 {
		t.Fatalf("third planned = %#v, want id 1", third)
	}
}

func TestMaxConcurrentNotExceededWithSparseSnapshot(t *testing.T) {
	ctx := context.Background()
	limiter := memory.NewConcurrencyLimiter()
	bases := []account.RoutingAccountBase{
		{Credential: account.Credential{
			ID: 1, Provider: account.ProviderBuild, AuthStatus: account.AuthStatusActive,
			Enabled: true, Priority: 100, MaxConcurrent: 1,
		}},
	}
	repo := &layeredAccountRepository{bases: bases, overlays: map[string]account.RoutingOverlaySnapshot{"model": {}}}
	selector := NewSelector(repo, limiter, memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)

	lease1, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lease1.Release()

	_, err = selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	var unavailable *SelectionUnavailableError
	if err == nil || !errors.As(err, &unavailable) || unavailable.Reason != SelectionSaturated {
		t.Fatalf("expected saturated when MaxConcurrent=1, got %v", err)
	}
}

func TestAccountScopedBaseInvalidationPatchesL3WithoutFullClear(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "l3-patch.db"))
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

	// Warm L3 assembled for model.
	warm, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", time.Now().UTC())
	if err != nil || len(warm) != 2 {
		t.Fatalf("warm candidates = %d err=%v", len(warm), err)
	}
	loadsAfterWarm := repo.listBaseCalls.Load()
	assembledBefore := selector.CacheStats().Sizes.AssembledEntries
	if assembledBefore < 1 {
		t.Fatal("expected warm assembled entry")
	}

	// Cooldown primary via MarkFailure (account-scoped base event).
	selector.MarkFailure(ctx, primary, 429, time.Hour)

	// L3 must still have an assembled entry (row-patched, not provider-cleared).
	stats := selector.CacheStats()
	if stats.Sizes.AssembledEntries < 1 {
		t.Fatalf("L3 was fully cleared after account-scoped base event: %#v", stats)
	}
	if stats.Assembled.Patches < 1 && stats.Invalidation.AssembledPatches < 1 {
		t.Fatalf("expected assembled patch counter: %#v", stats)
	}
	if stats.Invalidation.BulkRebuilds != 0 {
		t.Fatalf("unexpected bulk rebuild: %#v", stats.Invalidation)
	}

	// Other account remains selectable without full base reload.
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != backup.ID {
		t.Fatalf("selected %d want backup %d", lease.Credential.ID, backup.ID)
	}
	lease.Release()
	if repo.listBaseCalls.Load() != loadsAfterWarm {
		t.Fatalf("full base reload after account patch: warm=%d now=%d", loadsAfterWarm, repo.listBaseCalls.Load())
	}

	// Primary must not be selected while cooling.
	lease2, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", map[uint64]bool{backup.ID: true}, false)
	if err == nil {
		lease2.Release()
		t.Fatal("expected primary unavailable after failure cooldown")
	}
}

// overlayCountingRepo embeds countingListBasesRepo and counts overlay full-list loads.
type overlayCountingRepo struct {
	*countingListBasesRepo
	overlayLoads atomic.Int64
}

func (r *overlayCountingRepo) ListRoutingAccountOverlays(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel string) (account.RoutingOverlaySnapshot, error) {
	r.overlayLoads.Add(1)
	return r.countingListBasesRepo.ListRoutingAccountOverlays(ctx, provider, modelRouteID, upstreamModel)
}

func TestModelScopedOverlayPatchDoesNotReloadFullModelOverlay(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "overlay-patch.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inner := relational.NewAccountRepository(database)
	counting := &overlayCountingRepo{countingListBasesRepo: wrapCountingBases(inner)}

	a1, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "a1", SourceKey: "a1",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	a2, _, err := inner.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "a2", SourceKey: "a2",
		EncryptedAccessToken: "a", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 50, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	selector := NewSelector(counting, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	inner.SetInvalidationObserver(func(_ context.Context, event repository.InvalidationEvent) {
		selector.ApplyInvalidation(event)
	})

	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "chat-model", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	overlayLoadsWarm := counting.overlayLoads.Load()
	if overlayLoadsWarm < 1 {
		t.Fatal("expected warm overlay load")
	}
	assembledBefore := selector.CacheStats().Sizes.AssembledEntries

	// Model-scoped block on a1 only.
	selector.MarkModelAccessDenied(ctx, a1, "chat-model", time.Hour)

	stats := selector.CacheStats()
	if stats.Sizes.AssembledEntries < 1 && assembledBefore >= 1 {
		t.Fatalf("assembled fully cleared on model×account event: %#v", stats)
	}
	if stats.Invalidation.BulkRebuilds != 0 {
		t.Fatalf("unexpected bulk rebuild on overlay patch: %#v", stats.Invalidation)
	}

	// Next load must not force another ListRoutingAccountOverlays for the same model.
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "chat-model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != a2.ID {
		t.Fatalf("selected %d want a2 %d after a1 model block", lease.Credential.ID, a2.ID)
	}
	lease.Release()
	if counting.overlayLoads.Load() != overlayLoadsWarm {
		t.Fatalf("model×account patch forced full overlay reload: warm=%d now=%d", overlayLoadsWarm, counting.overlayLoads.Load())
	}

	// a1 blocked for this model.
	_, err = selector.Acquire(ctx, account.ProviderBuild, 0, "chat-model", "", "", map[uint64]bool{a2.ID: true}, false)
	if err == nil {
		t.Fatal("expected a1 model-cooling after access denied")
	}
	var unavailable *SelectionUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("want SelectionUnavailableError, got %v", err)
	}
	if unavailable.Reason != SelectionModelCooling && unavailable.Reason != SelectionNoAccounts && unavailable.Reason != SelectionCooling {
		// Model block yields model_cooling when only a1 remains and is blocked.
		t.Fatalf("unexpected reason %s for blocked a1", unavailable.Reason)
	}
}

func TestSegmentedSuccessPathConcurrencyBatchIsWindowSized(t *testing.T) {
	limiter := newSegmentedSelectiveLimiter()
	selector := newSegmentedActiveTestSelector(500, limiter, nil)
	selector.UpdateSegmentedSelector(true, 100, 16)

	lease, err := selector.Acquire(context.Background(), account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	sizes := limiter.BatchSizes()
	if len(sizes) != 1 {
		t.Fatalf("expected one concurrency snapshot on first-window success, got %v", sizes)
	}
	if sizes[0] > 16 {
		t.Fatalf("concurrency batch %d exceeds window 16 (must not be full pool 500)", sizes[0])
	}
	if sizes[0] == 500 {
		t.Fatal("concurrency snapshot covered full normal pool on successful window path")
	}
}

func TestStickyPreferenceStillHonoredWithSparseConcurrency(t *testing.T) {
	ctx := context.Background()
	limiter := memory.NewConcurrencyLimiter()
	bases := make([]account.RoutingAccountBase, 5)
	for i := range bases {
		bases[i] = account.RoutingAccountBase{Credential: account.Credential{
			ID: uint64(i + 1), Provider: account.ProviderBuild, AuthStatus: account.AuthStatusActive,
			Enabled: true, Priority: 10, MaxConcurrent: 2,
		}}
	}
	// Prefer higher priority on id 5 for non-sticky ordering noise.
	bases[4].Credential.Priority = 100
	repo := &layeredAccountRepository{bases: bases, overlays: map[string]account.RoutingOverlaySnapshot{"model": {}}}
	sticky := memory.NewStickyStore()
	selector := NewSelector(repo, limiter, sticky, nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	// stickySessionKey hashes affinity; Bind/Get use the same hash as Acquire.
	if err := sticky.Set(ctx, stickySessionKey("sess"), 2, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "sess", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Credential.ID != 2 {
		t.Fatalf("sticky preferred account = %d, want 2", lease.Credential.ID)
	}
}
