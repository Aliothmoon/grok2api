package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

func TestSelectorAssembledCacheReusesWhenEpochUnchanged(t *testing.T) {
	repo := newLayeredRepositoryFixture()
	selector := NewSelector(repo, nil, nil, nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	baseCalls, overlayCalls := repo.callCounts("model-a")
	if baseCalls != 1 || overlayCalls != 1 {
		t.Fatalf("expected cache hit base=%d overlay=%d", baseCalls, overlayCalls)
	}
}

func TestSelectorAssembledCacheMissesWhenEpochChanges(t *testing.T) {
	repo := newLayeredRepositoryFixture()
	selector := NewSelector(repo, nil, nil, nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountStateChanged, Provider: account.ProviderBuild, AccountID: 1,
	})
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	baseCalls, overlayCalls := repo.callCounts("model-a")
	if baseCalls != 2 || overlayCalls != 1 {
		t.Fatalf("state invalidation should reload base only base=%d overlay=%d", baseCalls, overlayCalls)
	}
}

func TestSelectorTokenCredentialInvalidationDoesNotReloadBase(t *testing.T) {
	repo := newLayeredRepositoryFixture()
	selector := NewSelector(repo, nil, nil, nil, time.Hour, time.Second, time.Minute)
	now := time.Now().UTC()
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	beforeBase, beforeOverlay := repo.callCounts("model-a")
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountCredentialChanged, Provider: account.ProviderBuild, AccountID: 1,
	})
	if _, err := selector.loadCandidates(context.Background(), account.ProviderBuild, 0, "model-a", "", now); err != nil {
		t.Fatal(err)
	}
	afterBase, afterOverlay := repo.callCounts("model-a")
	if afterBase != beforeBase || afterOverlay != beforeOverlay {
		t.Fatalf("token-only invalidation reloaded base=%d->%d overlay=%d->%d", beforeBase, afterBase, beforeOverlay, afterOverlay)
	}
}

func TestSelectorWriteAfterReadCooldownEligibility(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "write-after-read.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	primary, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backup, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "backup", SourceKey: "backup",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Wire invalidation into selector like production topology.
	selector := NewSelector(accounts, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	accounts.SetInvalidationObserver(func(_ context.Context, event repository.InvalidationEvent) {
		selector.ApplyInvalidation(event)
	})

	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != primary.ID {
		t.Fatalf("first selection = %d want %d", lease.Credential.ID, primary.ID)
	}
	lease.Release()

	selector.MarkFailure(ctx, primary, 429, time.Hour)
	lease, err = selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != backup.ID {
		t.Fatalf("after cooldown selection = %d want backup %d", lease.Credential.ID, backup.ID)
	}
	lease.Release()
}

func TestSelectorWriteAfterReadQuotaRecoveryEligibility(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "quota-after-write.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	primary, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backup, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "backup", SourceKey: "backup",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	selector := NewSelector(accounts, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	accounts.SetInvalidationObserver(func(_ context.Context, event repository.InvalidationEvent) {
		selector.ApplyInvalidation(event)
	})

	// Warm cache with both accounts eligible.
	if _, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	next := now.Add(time.Hour)
	// Force next probe into the future via repository to mirror exhausted recovery.
	if err := accounts.SaveQuotaRecovery(ctx, account.QuotaRecovery{
		AccountID: primary.ID, Kind: account.QuotaRecoveryKindFree, Status: account.QuotaRecoveryStatusExhausted,
		ConfirmedUsed: 1, ConfirmedLimit: 1, ExhaustedAt: &now, NextProbeAt: &next, LastConfirmedAt: &now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	selector.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountRecoveryChanged, Provider: account.ProviderBuild, AccountID: primary.ID,
	})

	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != backup.ID {
		t.Fatalf("after quota exhaust selection = %d want %d", lease.Credential.ID, backup.ID)
	}
	lease.Release()
}

func TestSelectorWriteAfterReadDisableEligibility(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "disable-after-write.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	primary, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 100, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backup, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "backup", SourceKey: "backup",
		EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true, Priority: 10, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	selector := NewSelector(accounts, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	accounts.SetInvalidationObserver(func(_ context.Context, event repository.InvalidationEvent) {
		selector.ApplyInvalidation(event)
	})
	if _, err := selector.loadCandidates(ctx, account.ProviderBuild, 0, "model", "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	enabled := false
	if _, err := accounts.UpdateMany(ctx, []uint64{primary.ID}, repository.AccountUpdates{Enabled: &enabled}); err != nil {
		t.Fatal(err)
	}
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != backup.ID {
		t.Fatalf("after disable selection = %d want %d", lease.Credential.ID, backup.ID)
	}
	lease.Release()
}

func TestAcquirePinnedUsesPointLookupWithoutFullPool(t *testing.T) {
	ctx := context.Background()
	repo := &pinnedLookupRepository{
		layeredAccountRepository: *newLayeredRepositoryFixture(),
		point: account.RoutingCandidate{
			Credential: account.Credential{
				ID: 42, Provider: account.ProviderBuild, Enabled: true, AuthStatus: account.AuthStatusActive, MaxConcurrent: 1,
			},
			ModelCapabilityKnown: true, SupportsModel: true,
		},
	}
	repo.combined = []account.RoutingCandidate{{Credential: account.Credential{ID: 99, Provider: account.ProviderBuild}}}
	selector := NewSelector(repo, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	lease, err := selector.AcquirePinned(ctx, account.ProviderBuild, 42, 0, "model-a", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != 42 {
		t.Fatalf("pinned lease = %#v", lease)
	}
	lease.Release()
	if repo.pointCalls != 1 {
		t.Fatalf("pointCalls = %d", repo.pointCalls)
	}
	if repo.combinedCalls != 0 || repo.baseCalls != 0 {
		t.Fatalf("pinned path loaded pool combined=%d base=%d", repo.combinedCalls, repo.baseCalls)
	}
}

func TestSelectorConcurrentAcquireAndInvalidate(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "concurrent-routing.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	for i := 0; i < 8; i++ {
		if _, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
			Provider: account.ProviderBuild, Name: "acc", SourceKey: "acc-" + string(rune('a'+i)),
			EncryptedAccessToken: "access", AuthStatus: account.AuthStatusActive, Enabled: true,
			Priority: 10 + i, MaxConcurrent: 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	selector := NewSelector(accounts, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	accounts.SetInvalidationObserver(func(_ context.Context, event repository.InvalidationEvent) {
		selector.ApplyInvalidation(event)
	})

	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
				if err != nil {
					var unavailable *SelectionUnavailableError
					if errors.As(err, &unavailable) {
						continue
					}
					errCh <- err
					return
				}
				if account.HasRoutingSecrets(lease.Credential) {
					errCh <- errors.New("lease leaked routing secrets")
					lease.Release()
					return
				}
				if j%3 == 0 {
					selector.MarkFailure(ctx, lease.Credential, 429, 50*time.Millisecond)
				}
				lease.Release()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			selector.ApplyInvalidation(repository.InvalidationEvent{
				Kind: repository.InvalidationAccountStateChanged, Provider: account.ProviderBuild,
			})
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

type pinnedLookupRepository struct {
	layeredAccountRepository
	point      account.RoutingCandidate
	pointCalls int
}

func (r *pinnedLookupRepository) GetRoutingCandidate(context.Context, uint64, uint64, string, string) (account.RoutingCandidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pointCalls++
	return r.point, nil
}

func (r *pinnedLookupRepository) GetRoutingAccountBase(context.Context, uint64, string) (account.RoutingAccountBase, error) {
	return account.RoutingAccountBase{Credential: r.point.Credential}, nil
}

// Ensure interfaces are implemented.
var (
	_ repository.RoutingLayerRepository = (*layeredAccountRepository)(nil)
	_ repository.RoutingAccountLookup   = (*pinnedLookupRepository)(nil)
)
