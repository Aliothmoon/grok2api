package gateway

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
)

func TestSelectionStatsStoreSnapshotAndReset(t *testing.T) {
	store := newSelectionStatsStore()
	store.recordClaim(account.ProviderBuild, 1, "alpha", selectionPathHeap)
	store.recordClaim(account.ProviderBuild, 1, "alpha", selectionPathHeap)
	store.recordClaim(account.ProviderBuild, 2, "beta", selectionPathSticky)
	store.recordClaim(account.ProviderWeb, 9, "web", selectionPathHeap)
	store.recordUpstreamStarted(account.ProviderBuild, 1)
	store.recordUpstreamOutcome(account.ProviderBuild, 1, "success")
	store.recordUpstreamStarted(account.ProviderBuild, 2)
	store.recordUpstreamOutcome(account.ProviderBuild, 2, "failed")

	view := store.Snapshot(1)
	if view.Totals.Claims != 4 {
		t.Fatalf("claims=%d", view.Totals.Claims)
	}
	if view.Totals.ByPath[selectionPathHeap] != 3 || view.Totals.ByPath[selectionPathSticky] != 1 {
		t.Fatalf("by_path=%#v", view.Totals.ByPath)
	}
	if view.Skew.UniqueAccountsClaimed != 3 {
		t.Fatalf("unique=%d", view.Skew.UniqueAccountsClaimed)
	}
	if view.Skew.Top1ClaimRatio < 0.49 || view.Skew.Top1ClaimRatio > 0.51 {
		t.Fatalf("top1=%v", view.Skew.Top1ClaimRatio)
	}
	build := view.ByProvider[string(account.ProviderBuild)]
	if build.Claims != 3 || len(build.Accounts) != 1 || build.Accounts[0].AccountID != 1 {
		t.Fatalf("build top=%#v", build)
	}
	if build.Other == nil || build.Other.Accounts != 1 || build.Other.Claims != 1 {
		t.Fatalf("build other=%#v", build.Other)
	}
	if build.Accounts[0].Name != "alpha" || build.Accounts[0].UpstreamSuccess != 1 {
		t.Fatalf("account stats=%#v", build.Accounts[0])
	}

	store.Reset()
	after := store.Snapshot(10)
	if after.Totals.Claims != 0 || after.Window.Resets != 1 || len(after.ByProvider) != 0 {
		t.Fatalf("after reset=%#v", after)
	}
}

func TestAcquireRecordsSelectionStats(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "selection-stats.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	primary, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "primary", SourceKey: "primary", EncryptedAccessToken: "encrypted",
		Enabled: true, AuthStatus: account.AuthStatusActive, Priority: 10, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondary, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, Name: "secondary", SourceKey: "secondary", EncryptedAccessToken: "encrypted",
		Enabled: true, AuthStatus: account.AuthStatusActive, Priority: 1, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	selector := NewSelector(accounts, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	lease, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Credential.ID != primary.ID {
		t.Fatalf("selected=%d want=%d", lease.Credential.ID, primary.ID)
	}
	lease.markSelectorUpstreamStarted()
	lease.completeSelectorObservation(true)
	lease.Release()

	view := selector.SelectionStats(10)
	if view.Totals.Claims != 1 || view.Totals.UpstreamSuccess != 1 {
		t.Fatalf("totals=%#v", view.Totals)
	}
	if view.Totals.ByPath[selectionPathHeap] != 1 {
		t.Fatalf("paths=%#v", view.Totals.ByPath)
	}
	build := view.ByProvider[string(account.ProviderBuild)]
	if len(build.Accounts) != 1 || build.Accounts[0].AccountID != primary.ID || build.Accounts[0].ClaimRatio != 1 {
		t.Fatalf("accounts=%#v", build.Accounts)
	}

	selector.ResetSelectionStats()
	sticky := memory.NewStickyStore()
	limiter := memory.NewConcurrencyLimiter()
	selector = NewSelector(accounts, limiter, sticky, nil, time.Hour, time.Second, time.Minute)
	if err := sticky.Set(ctx, stickySessionKey("aff"), primary.ID, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	hold, ok, err := limiter.Acquire(ctx, accountConcurrencyKey(primary.ID), 2)
	if err != nil || !ok {
		t.Fatalf("hold primary: ok=%v err=%v", ok, err)
	}
	hold2, ok, err := limiter.Acquire(ctx, accountConcurrencyKey(primary.ID), 2)
	if err != nil || !ok {
		t.Fatalf("hold primary 2: ok=%v err=%v", ok, err)
	}
	borrow, err := selector.Acquire(ctx, account.ProviderBuild, 0, "model", "", "aff", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if borrow.Credential.ID != secondary.ID {
		t.Fatalf("borrowed=%d want=%d", borrow.Credential.ID, secondary.ID)
	}
	borrow.Release()
	hold()
	hold2()
	view = selector.SelectionStats(10)
	if view.Totals.Claims != 1 || view.Totals.ByPath[selectionPathStickyBorrow] != 1 {
		t.Fatalf("borrow stats=%#v", view)
	}
}

func TestNormalizeSelectionPath(t *testing.T) {
	if got := normalizeSelectionPath("first_window"); got != selectionPathSegmentedFirstWindow {
		t.Fatalf("got=%s", got)
	}
	if got := normalizeSelectionPath(selectionPathPinned); got != selectionPathPinned {
		t.Fatalf("got=%s", got)
	}
}
