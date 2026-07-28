package gateway

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
)

// Selection path labels for business metrics. Keep the set small and stable.
const (
	selectionPathHeap                   = "heap"
	selectionPathSticky                 = "sticky"
	selectionPathStickyBind             = "sticky_bind"
	selectionPathStickyBorrow           = "sticky_borrow"
	selectionPathPinned                 = "pinned"
	selectionPathProbe                  = "probe"
	selectionPathSegmentedFirstWindow   = "segmented_first_window"
	selectionPathSegmentedLaterWindow   = "segmented_later_window"
	selectionPathSegmentedLaterCohort   = "segmented_later_cohort"
	selectionPathSegmentedFullFallback  = "segmented_full_fallback"
)

const defaultSelectionStatsTopN = 50

// SelectionAccountStats is one account's counters in a snapshot.
type SelectionAccountStats struct {
	AccountID             uint64             `json:"account_id"`
	Name                  string             `json:"name,omitempty"`
	Claims                uint64             `json:"claims"`
	ClaimRatio            float64            `json:"claim_ratio"`
	UpstreamStarted       uint64             `json:"upstream_started"`
	UpstreamSuccess       uint64             `json:"upstream_success"`
	UpstreamFailed        uint64             `json:"upstream_failed"`
	UpstreamSkipped       uint64             `json:"upstream_skipped"`
	UpstreamSuccessRatio  float64            `json:"upstream_success_ratio"`
	Paths                 map[string]uint64  `json:"paths,omitempty"`
}

// SelectionOtherStats aggregates accounts outside the top-N list.
type SelectionOtherStats struct {
	Accounts   int     `json:"accounts"`
	Claims     uint64  `json:"claims"`
	ClaimRatio float64 `json:"claim_ratio"`
}

// SelectionProviderStats groups selection counters by provider.
type SelectionProviderStats struct {
	Claims   uint64                  `json:"claims"`
	Accounts []SelectionAccountStats `json:"accounts"`
	Other    *SelectionOtherStats    `json:"other,omitempty"`
}

// SelectionSkewStats summarizes concentration of claims.
type SelectionSkewStats struct {
	Top1ClaimRatio         float64 `json:"top1_claim_ratio"`
	Top5ClaimRatio         float64 `json:"top5_claim_ratio"`
	UniqueAccountsClaimed  int     `json:"unique_accounts_claimed"`
}

// SelectionTotals is process-local aggregate counters.
type SelectionTotals struct {
	Claims            uint64            `json:"claims"`
	UpstreamStarted   uint64            `json:"upstream_started"`
	UpstreamSuccess   uint64            `json:"upstream_success"`
	UpstreamFailed    uint64            `json:"upstream_failed"`
	UpstreamSkipped   uint64            `json:"upstream_skipped"`
	ByPath            map[string]uint64 `json:"by_path,omitempty"`
}

// SelectionWindow describes the counter accumulation window.
type SelectionWindow struct {
	StartedAt time.Time `json:"started_at"`
	Resets    uint64    `json:"resets"`
	Note      string    `json:"note"`
}

// SelectionStatsView is exposed via GET /debug/selector/stats.
type SelectionStatsView struct {
	Window     SelectionWindow                    `json:"window"`
	Totals     SelectionTotals                    `json:"totals"`
	Skew       SelectionSkewStats                 `json:"skew"`
	ByProvider map[string]SelectionProviderStats  `json:"by_provider"`
}

type selectionAccountKey struct {
	provider account.Provider
	id       uint64
}

type selectionAccountCounter struct {
	name              atomic.Value // string
	claims            atomic.Uint64
	upstreamStarted   atomic.Uint64
	upstreamSuccess   atomic.Uint64
	upstreamFailed    atomic.Uint64
	upstreamSkipped   atomic.Uint64
	pathMu            sync.Mutex
	pathClaims        map[string]*atomic.Uint64
}

type selectionStatsStore struct {
	startedAt atomic.Value // time.Time
	resets    atomic.Uint64

	totalClaims          atomic.Uint64
	totalUpstreamStarted atomic.Uint64
	totalUpstreamSuccess atomic.Uint64
	totalUpstreamFailed  atomic.Uint64
	totalUpstreamSkipped atomic.Uint64

	pathMu     sync.Mutex
	pathTotals map[string]*atomic.Uint64

	accounts sync.Map // selectionAccountKey -> *selectionAccountCounter
}

func newSelectionStatsStore() *selectionStatsStore {
	store := &selectionStatsStore{
		pathTotals: make(map[string]*atomic.Uint64),
	}
	store.startedAt.Store(time.Now().UTC())
	return store
}

func (s *selectionStatsStore) recordClaim(provider account.Provider, accountID uint64, name, path string) {
	if s == nil || accountID == 0 || provider == "" {
		return
	}
	if path == "" {
		path = selectionPathHeap
	}
	s.totalClaims.Add(1)
	s.bumpPath(path)
	counter := s.account(provider, accountID)
	if name != "" {
		if existing, ok := counter.name.Load().(string); !ok || existing == "" {
			counter.name.Store(name)
		}
	}
	counter.claims.Add(1)
	counter.bumpPath(path)
}

func (s *selectionStatsStore) recordUpstreamStarted(provider account.Provider, accountID uint64) {
	if s == nil || accountID == 0 {
		return
	}
	s.totalUpstreamStarted.Add(1)
	s.account(provider, accountID).upstreamStarted.Add(1)
}

func (s *selectionStatsStore) recordUpstreamOutcome(provider account.Provider, accountID uint64, outcome string) {
	if s == nil || accountID == 0 {
		return
	}
	counter := s.account(provider, accountID)
	switch outcome {
	case "success":
		s.totalUpstreamSuccess.Add(1)
		counter.upstreamSuccess.Add(1)
	case "failed":
		s.totalUpstreamFailed.Add(1)
		counter.upstreamFailed.Add(1)
	case "skipped":
		s.totalUpstreamSkipped.Add(1)
		counter.upstreamSkipped.Add(1)
	}
}

func (s *selectionStatsStore) account(provider account.Provider, accountID uint64) *selectionAccountCounter {
	key := selectionAccountKey{provider: provider, id: accountID}
	if existing, ok := s.accounts.Load(key); ok {
		return existing.(*selectionAccountCounter)
	}
	created := &selectionAccountCounter{pathClaims: make(map[string]*atomic.Uint64)}
	actual, _ := s.accounts.LoadOrStore(key, created)
	return actual.(*selectionAccountCounter)
}

func (s *selectionStatsStore) bumpPath(path string) {
	s.pathMu.Lock()
	counter, ok := s.pathTotals[path]
	if !ok {
		counter = &atomic.Uint64{}
		s.pathTotals[path] = counter
	}
	s.pathMu.Unlock()
	counter.Add(1)
}

func (c *selectionAccountCounter) bumpPath(path string) {
	c.pathMu.Lock()
	counter, ok := c.pathClaims[path]
	if !ok {
		counter = &atomic.Uint64{}
		c.pathClaims[path] = counter
	}
	c.pathMu.Unlock()
	counter.Add(1)
}

func (c *selectionAccountCounter) pathSnapshot() map[string]uint64 {
	c.pathMu.Lock()
	defer c.pathMu.Unlock()
	if len(c.pathClaims) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(c.pathClaims))
	for path, counter := range c.pathClaims {
		if value := counter.Load(); value > 0 {
			out[path] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *selectionStatsStore) Reset() {
	if s == nil {
		return
	}
	s.totalClaims.Store(0)
	s.totalUpstreamStarted.Store(0)
	s.totalUpstreamSuccess.Store(0)
	s.totalUpstreamFailed.Store(0)
	s.totalUpstreamSkipped.Store(0)
	s.resets.Add(1)
	s.startedAt.Store(time.Now().UTC())

	s.pathMu.Lock()
	s.pathTotals = make(map[string]*atomic.Uint64)
	s.pathMu.Unlock()

	s.accounts.Range(func(key, _ any) bool {
		s.accounts.Delete(key)
		return true
	})
}

func (s *selectionStatsStore) Snapshot(topN int) SelectionStatsView {
	if s == nil {
		return SelectionStatsView{
			Window: SelectionWindow{
				StartedAt: time.Time{},
				Note:      "process-local cumulative counters since start or last reset",
			},
			ByProvider: map[string]SelectionProviderStats{},
		}
	}
	if topN <= 0 {
		topN = defaultSelectionStatsTopN
	}

	startedAt, _ := s.startedAt.Load().(time.Time)
	totals := SelectionTotals{
		Claims:          s.totalClaims.Load(),
		UpstreamStarted: s.totalUpstreamStarted.Load(),
		UpstreamSuccess: s.totalUpstreamSuccess.Load(),
		UpstreamFailed:  s.totalUpstreamFailed.Load(),
		UpstreamSkipped: s.totalUpstreamSkipped.Load(),
		ByPath:          s.pathTotalsSnapshot(),
	}

	type ranked struct {
		provider account.Provider
		stats    SelectionAccountStats
	}
	byProviderRaw := make(map[account.Provider][]ranked)
	var allClaims []uint64

	s.accounts.Range(func(key, value any) bool {
		accountKey := key.(selectionAccountKey)
		counter := value.(*selectionAccountCounter)
		claims := counter.claims.Load()
		if claims == 0 && counter.upstreamStarted.Load() == 0 && counter.upstreamSuccess.Load() == 0 &&
			counter.upstreamFailed.Load() == 0 && counter.upstreamSkipped.Load() == 0 {
			return true
		}
		name, _ := counter.name.Load().(string)
		item := SelectionAccountStats{
			AccountID:       accountKey.id,
			Name:            name,
			Claims:          claims,
			UpstreamStarted: counter.upstreamStarted.Load(),
			UpstreamSuccess: counter.upstreamSuccess.Load(),
			UpstreamFailed:  counter.upstreamFailed.Load(),
			UpstreamSkipped: counter.upstreamSkipped.Load(),
			Paths:           counter.pathSnapshot(),
		}
		if totals.Claims > 0 {
			item.ClaimRatio = float64(claims) / float64(totals.Claims)
		}
		if item.UpstreamStarted > 0 {
			item.UpstreamSuccessRatio = float64(item.UpstreamSuccess) / float64(item.UpstreamStarted)
		}
		byProviderRaw[accountKey.provider] = append(byProviderRaw[accountKey.provider], ranked{
			provider: accountKey.provider,
			stats:    item,
		})
		if claims > 0 {
			allClaims = append(allClaims, claims)
		}
		return true
	})

	byProvider := make(map[string]SelectionProviderStats, len(byProviderRaw))
	for provider, items := range byProviderRaw {
		sort.Slice(items, func(left, right int) bool {
			if items[left].stats.Claims != items[right].stats.Claims {
				return items[left].stats.Claims > items[right].stats.Claims
			}
			return items[left].stats.AccountID < items[right].stats.AccountID
		})
		var providerClaims uint64
		for _, item := range items {
			providerClaims += item.stats.Claims
		}
		keep := topN
		if keep > len(items) {
			keep = len(items)
		}
		accounts := make([]SelectionAccountStats, 0, keep)
		for index := 0; index < keep; index++ {
			accounts = append(accounts, items[index].stats)
		}
		var other *SelectionOtherStats
		if keep < len(items) {
			var otherClaims uint64
			for index := keep; index < len(items); index++ {
				otherClaims += items[index].stats.Claims
			}
			other = &SelectionOtherStats{
				Accounts: len(items) - keep,
				Claims:   otherClaims,
			}
			if totals.Claims > 0 {
				other.ClaimRatio = float64(otherClaims) / float64(totals.Claims)
			}
		}
		byProvider[string(provider)] = SelectionProviderStats{
			Claims:   providerClaims,
			Accounts: accounts,
			Other:    other,
		}
	}

	return SelectionStatsView{
		Window: SelectionWindow{
			StartedAt: startedAt,
			Resets:    s.resets.Load(),
			Note:      "process-local cumulative counters since start or last reset",
		},
		Totals:     totals,
		Skew:       computeSelectionSkew(allClaims),
		ByProvider: byProvider,
	}
}

func (s *selectionStatsStore) pathTotalsSnapshot() map[string]uint64 {
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	if len(s.pathTotals) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(s.pathTotals))
	for path, counter := range s.pathTotals {
		if value := counter.Load(); value > 0 {
			out[path] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func computeSelectionSkew(claims []uint64) SelectionSkewStats {
	if len(claims) == 0 {
		return SelectionSkewStats{}
	}
	sort.Slice(claims, func(left, right int) bool { return claims[left] > claims[right] })
	var total uint64
	for _, value := range claims {
		total += value
	}
	skew := SelectionSkewStats{UniqueAccountsClaimed: len(claims)}
	if total == 0 {
		return skew
	}
	skew.Top1ClaimRatio = float64(claims[0]) / float64(total)
	var top5 uint64
	limit := min(5, len(claims))
	for index := 0; index < limit; index++ {
		top5 += claims[index]
	}
	skew.Top5ClaimRatio = float64(top5) / float64(total)
	return skew
}

func normalizeSelectionPath(stage string) string {
	switch stage {
	case "first_window":
		return selectionPathSegmentedFirstWindow
	case "later_window":
		return selectionPathSegmentedLaterWindow
	case "later_cohort":
		return selectionPathSegmentedLaterCohort
	case "full_fallback":
		return selectionPathSegmentedFullFallback
	case selectionPathHeap, selectionPathSticky, selectionPathStickyBind, selectionPathStickyBorrow,
		selectionPathPinned, selectionPathProbe,
		selectionPathSegmentedFirstWindow, selectionPathSegmentedLaterWindow,
		selectionPathSegmentedLaterCohort, selectionPathSegmentedFullFallback:
		return stage
	default:
		if stage == "" {
			return selectionPathHeap
		}
		return stage
	}
}
