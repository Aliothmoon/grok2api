package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/pkg/resultcache"
	"github.com/chenyme/grok2api/backend/internal/repository"
	"golang.org/x/sync/singleflight"
)

type accountLease struct {
	Credential          account.Credential
	Billing             *account.Billing
	QuotaProbe          bool
	QuotaProbeKind      account.QuotaRecoveryKind
	QuotaMode           string
	selectorObservation *selectorLeaseObservation
	release             func()
}

const quotaProbeLease = 5 * time.Minute
const successPersistInterval = 30 * time.Second
// candidateCacheSoftTTL is only a defensive refresh bound for single-node memory
// caches. Correctness relies on epoch/version matching after invalidation, not this TTL.
const candidateCacheSoftTTL = 5 * time.Minute
const concurrencySnapshotTTL = 25 * time.Millisecond
const maxConcurrencySnapshots = 256
// routingBasePointLoadTimeout bounds GetRoutingAccountBase during sync invalidation.
const routingBasePointLoadTimeout = 3 * time.Second

const modelAccessDeniedCooldown = 5 * time.Minute

const defaultFreeQuotaRecoveryPause = 24 * time.Hour

type quotaRecoveryHints struct {
	Billing *account.Billing
}

type candidateSnapshot struct {
	values    []account.RoutingCandidate
	byAccount map[uint64]int
	baseGen   routingLayerVersion
	overlayGen routingLayerVersion
	loadedAt  time.Time
}

func newCandidateSnapshot(values []account.RoutingCandidate, baseGen, overlayGen routingLayerVersion, loadedAt time.Time) candidateSnapshot {
	byAccount := make(map[uint64]int, len(values))
	for index, value := range values {
		if _, exists := byAccount[value.Credential.ID]; !exists {
			byAccount[value.Credential.ID] = index
		}
	}
	return candidateSnapshot{values: values, byAccount: byAccount, baseGen: baseGen, overlayGen: overlayGen, loadedAt: loadedAt}
}

type candidateCacheKey struct {
	provider      account.Provider
	modelRouteID  uint64
	upstreamModel string
	quotaMode     string
}

type routingBaseCacheKey struct {
	provider  account.Provider
	quotaMode string
}

type routingOverlayCacheKey struct {
	provider      account.Provider
	modelRouteID  uint64
	upstreamModel string
}

type routingLayerVersion struct {
	global   uint64
	provider uint64
}

type routingBaseSnapshot struct {
	values   []account.RoutingAccountBase
	version  routingLayerVersion
	loadedAt time.Time
}

type routingOverlaySnapshot struct {
	value    account.RoutingOverlaySnapshot
	version  routingLayerVersion
	loadedAt time.Time
}

type SelectionUnavailableReason string

const (
	SelectionNoAccounts       SelectionUnavailableReason = "no_accounts"
	SelectionUnsupportedModel SelectionUnavailableReason = "unsupported_model"
	SelectionCooling          SelectionUnavailableReason = "cooling"
	SelectionModelCooling     SelectionUnavailableReason = "model_cooling"
	SelectionQuotaExhausted   SelectionUnavailableReason = "quota_exhausted"
	SelectionSaturated        SelectionUnavailableReason = "saturated"
)

// SelectionUnavailableError 保留选号失败的真实原因，避免所有情况都退化成模糊的 503。
type SelectionUnavailableError struct {
	Reason     SelectionUnavailableReason
	RetryAfter time.Duration
}

func (e *SelectionUnavailableError) Error() string {
	if e == nil {
		return "没有可用上游账号"
	}
	switch e.Reason {
	case SelectionUnsupportedModel:
		return "当前账号池不支持该模型"
	case SelectionCooling:
		return "可用上游账号正在冷却"
	case SelectionModelCooling:
		return "可用上游账号的目标模型正在冷却"
	case SelectionQuotaExhausted:
		return "可用上游账号额度等待恢复"
	case SelectionSaturated:
		return "可用上游账号均达到并发上限"
	default:
		return "没有可用上游账号"
	}
}

func (l *accountLease) Release() {
	if l == nil {
		return
	}
	if l.selectorObservation != nil {
		l.selectorObservation.completeRelease()
	}
	if l.release != nil {
		l.release()
		l.release = nil
	}
}

func (l *accountLease) markSelectorUpstreamStarted() {
	if l != nil && l.selectorObservation != nil {
		l.selectorObservation.upstreamStarted.Store(true)
	}
}

func (l *accountLease) completeSelectorObservation(success bool) {
	if l != nil && l.selectorObservation != nil {
		l.selectorObservation.complete(success)
	}
}

// Selector 实现可替换的 balanced 账号选择策略。
type Selector struct {
	accounts               repository.AccountRepository
	concurrency            repository.ConcurrencyLimiter
	sticky                 repository.StickySessionRepository
	stickyTTL              time.Duration
	cooldownBase           time.Duration
	cooldownMax            time.Duration
	capacityWait           time.Duration
	preferFreeBuild        bool
	segmentedConfig        segmentedSelectorConfig
	segmentedState         segmentedSelectorState
	configMu               sync.RWMutex
	candidateMu            sync.Mutex
	selectionMu            sync.RWMutex
	leaseWakeMu            sync.Mutex
	leaseWake              chan struct{}
	lastSelectedAt         map[uint64]time.Time
	lastSuccessAt          map[uint64]time.Time
	candidates             map[candidateCacheKey]candidateSnapshot
	routingBases           map[routingBaseCacheKey]routingBaseSnapshot
	routingOverlays        map[routingOverlayCacheKey]routingOverlaySnapshot
	baseGlobalVersion      uint64
	overlayGlobalVersion   uint64
	baseProviderVersion    map[account.Provider]uint64
	overlayProviderVersion map[account.Provider]uint64
	candidateLoads         singleflight.Group
	concurrencySnapshots   *resultcache.Cache[[32]byte, map[uint64]int]
	cacheStats             routingCacheStats
	tierOrders             interface {
		TierOrder(account.Provider, string) []account.WebTier
	}
}

// RoutingCacheLayerStats is a snapshot of hit/miss/load counters for one cache layer.
type RoutingCacheLayerStats struct {
	Hits     uint64   `json:"hits"`
	Misses   uint64   `json:"misses"`
	Loads    uint64   `json:"loads"`
	Patches  uint64   `json:"patches,omitempty"`
	Rebuilds uint64   `json:"rebuilds,omitempty"`
	HitRatio *float64 `json:"hit_ratio"`
}

// RoutingCacheStats is exposed via /debug/cache/stats when pprof is enabled.
type RoutingCacheStats struct {
	Assembled    RoutingCacheLayerStats `json:"assembled"`
	Base         RoutingCacheLayerStats `json:"base"`
	Overlay      RoutingCacheLayerStats `json:"overlay"`
	Invalidation struct {
		TokenOnlySkipped uint64 `json:"token_only_skipped"`
		BaseEvents       uint64 `json:"base_events"`
		OverlayEvents    uint64 `json:"overlay_events"`
		BulkRebuilds     uint64 `json:"bulk_rebuilds"`
		AssembledPatches uint64 `json:"assembled_patches"`
		OverlayPatches   uint64 `json:"overlay_patches"`
	} `json:"invalidation"`
	Sizes struct {
		AssembledEntries int            `json:"assembled_entries"`
		BaseSnapshots    int            `json:"base_snapshots"`
		OverlaySnapshots int            `json:"overlay_snapshots"`
		BaseAccounts     map[string]int `json:"base_accounts_by_provider,omitempty"`
	} `json:"sizes"`
}

type routingCacheStats struct {
	assembledHits, assembledMisses, assembledLoads atomic.Uint64
	baseHits, baseMisses, baseLoads                atomic.Uint64
	basePatches, baseRebuilds                      atomic.Uint64
	overlayHits, overlayMisses, overlayLoads       atomic.Uint64
	overlayPatches                                 atomic.Uint64
	assembledPatches                               atomic.Uint64
	tokenOnlySkipped, baseEvents, overlayEvents    atomic.Uint64
	bulkRebuilds                                   atomic.Uint64
}

func layerStats(hits, misses, loads, patches, rebuilds uint64) RoutingCacheLayerStats {
	stats := RoutingCacheLayerStats{Hits: hits, Misses: misses, Loads: loads, Patches: patches, Rebuilds: rebuilds}
	total := hits + misses
	if total > 0 {
		ratio := float64(hits) / float64(total)
		stats.HitRatio = &ratio
	}
	return stats
}

func NewSelector(accounts repository.AccountRepository, concurrency repository.ConcurrencyLimiter, sticky repository.StickySessionRepository, tierOrders interface {
	TierOrder(account.Provider, string) []account.WebTier
}, stickyTTL, cooldownBase, cooldownMax time.Duration, capacityWait ...time.Duration) *Selector {
	wait := time.Duration(0)
	if len(capacityWait) > 0 && capacityWait[0] > 0 {
		wait = capacityWait[0]
	}
	return &Selector{accounts: accounts, concurrency: concurrency, sticky: sticky, tierOrders: tierOrders, stickyTTL: stickyTTL, cooldownBase: cooldownBase, cooldownMax: cooldownMax, capacityWait: wait, leaseWake: make(chan struct{}), lastSelectedAt: make(map[uint64]time.Time), lastSuccessAt: make(map[uint64]time.Time), candidates: make(map[candidateCacheKey]candidateSnapshot), routingBases: make(map[routingBaseCacheKey]routingBaseSnapshot), routingOverlays: make(map[routingOverlayCacheKey]routingOverlaySnapshot), baseProviderVersion: make(map[account.Provider]uint64), overlayProviderVersion: make(map[account.Provider]uint64), concurrencySnapshots: resultcache.New[[32]byte, map[uint64]int](maxConcurrencySnapshots, concurrencySnapshotTTL)}
}

func (s *Selector) UpdateConfig(stickyTTL, cooldownBase, cooldownMax time.Duration, capacityWait ...time.Duration) {
	s.configMu.Lock()
	s.stickyTTL = stickyTTL
	s.cooldownBase = cooldownBase
	s.cooldownMax = cooldownMax
	if len(capacityWait) > 0 {
		s.capacityWait = max(time.Duration(0), capacityWait[0])
	}
	s.configMu.Unlock()
}

// UpdatePreferFreeBuild 热更新 Build Free 账号优先策略。
func (s *Selector) UpdatePreferFreeBuild(value bool) {
	s.configMu.Lock()
	s.preferFreeBuild = value
	s.configMu.Unlock()
}

// UpdateSegmentedSelector changes the large-pool bounded planner policy.
func (s *Selector) UpdateSegmentedSelector(enabled bool, minCandidates, windowSize int) {
	s.configMu.Lock()
	s.segmentedConfig = normalizeSegmentedSelectorConfig(segmentedSelectorConfig{
		enabled: enabled, minCandidates: minCandidates, windowSize: windowSize,
	})
	s.configMu.Unlock()
}

func (s *Selector) routingConfig() (time.Duration, time.Duration, time.Duration, time.Duration) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.stickyTTL, s.cooldownBase, s.cooldownMax, s.capacityWait
}

func (s *Selector) preferFreeBuildEnabled() bool {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.preferFreeBuild
}

// CacheStats returns atomic counters and current in-memory cache sizes.
func (s *Selector) CacheStats() RoutingCacheStats {
	if s == nil {
		return RoutingCacheStats{}
	}
	out := RoutingCacheStats{
		Assembled: layerStats(s.cacheStats.assembledHits.Load(), s.cacheStats.assembledMisses.Load(), s.cacheStats.assembledLoads.Load(), s.cacheStats.assembledPatches.Load(), 0),
		Base:      layerStats(s.cacheStats.baseHits.Load(), s.cacheStats.baseMisses.Load(), s.cacheStats.baseLoads.Load(), s.cacheStats.basePatches.Load(), s.cacheStats.baseRebuilds.Load()),
		Overlay:   layerStats(s.cacheStats.overlayHits.Load(), s.cacheStats.overlayMisses.Load(), s.cacheStats.overlayLoads.Load(), s.cacheStats.overlayPatches.Load(), 0),
	}
	out.Invalidation.TokenOnlySkipped = s.cacheStats.tokenOnlySkipped.Load()
	out.Invalidation.BaseEvents = s.cacheStats.baseEvents.Load()
	out.Invalidation.OverlayEvents = s.cacheStats.overlayEvents.Load()
	out.Invalidation.BulkRebuilds = s.cacheStats.bulkRebuilds.Load()
	out.Invalidation.AssembledPatches = s.cacheStats.assembledPatches.Load()
	out.Invalidation.OverlayPatches = s.cacheStats.overlayPatches.Load()
	s.candidateMu.Lock()
	out.Sizes.AssembledEntries = len(s.candidates)
	out.Sizes.BaseSnapshots = len(s.routingBases)
	out.Sizes.OverlaySnapshots = len(s.routingOverlays)
	if len(s.routingBases) > 0 {
		out.Sizes.BaseAccounts = make(map[string]int, len(s.routingBases))
		for key, snap := range s.routingBases {
			out.Sizes.BaseAccounts[string(key.provider)+"/"+key.quotaMode] = len(snap.values)
		}
	}
	s.candidateMu.Unlock()
	return out
}

// ResetCacheStats zeroes hit/miss/load counters (debug sampling helper).
func (s *Selector) ResetCacheStats() {
	if s == nil {
		return
	}
	s.cacheStats.assembledHits.Store(0)
	s.cacheStats.assembledMisses.Store(0)
	s.cacheStats.assembledLoads.Store(0)
	s.cacheStats.baseHits.Store(0)
	s.cacheStats.baseMisses.Store(0)
	s.cacheStats.baseLoads.Store(0)
	s.cacheStats.basePatches.Store(0)
	s.cacheStats.baseRebuilds.Store(0)
	s.cacheStats.overlayHits.Store(0)
	s.cacheStats.overlayMisses.Store(0)
	s.cacheStats.overlayLoads.Store(0)
	s.cacheStats.overlayPatches.Store(0)
	s.cacheStats.assembledPatches.Store(0)
	s.cacheStats.tokenOnlySkipped.Store(0)
	s.cacheStats.baseEvents.Store(0)
	s.cacheStats.overlayEvents.Store(0)
	s.cacheStats.bulkRebuilds.Store(0)
}

func (s *Selector) Acquire(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel, quotaMode, affinityKey string, excluded map[uint64]bool, allowQuotaProbe bool) (*accountLease, error) {
	now := time.Now().UTC()
	stickyKey := stickySessionKey(affinityKey)
	values, err := s.loadCandidates(ctx, provider, modelRouteID, upstreamModel, quotaMode, now)
	if err != nil {
		return nil, err
	}
	// 仅保留候选下标，避免每个请求复制包含凭据、计费和额度结构的完整账号切片。
	normalCandidates := make([]int, 0, len(values))
	probeCandidates := make([]int, 0, len(values))
	supportedCandidates := 0
	consideredCandidates := 0
	coolingCandidates := 0
	modelCoolingCandidates := 0
	quotaCandidates := 0
	var earliestRetry time.Time
	for index, candidate := range values {
		value := candidate.Credential
		if excluded[value.ID] || value.AuthStatus != account.AuthStatusActive {
			continue
		}
		consideredCandidates++
		if candidate.ModelCapabilityKnown && !candidate.SupportsModel {
			continue
		}
		supportedCandidates++
		if candidate.ModelQuotaBlock != nil && now.Before(candidate.ModelQuotaBlock.CooldownUntil) {
			modelCoolingCandidates++
			earliestRetry = earlierFuture(earliestRetry, candidate.ModelQuotaBlock.CooldownUntil, now)
			continue
		}
		if value.CooldownUntil != nil && now.Before(*value.CooldownUntil) {
			coolingCandidates++
			earliestRetry = earlierFuture(earliestRetry, *value.CooldownUntil, now)
			continue
		}
		quotaRecovery := candidate.QuotaRecovery
		if quotaRecovery != nil && quotaRecovery.Status != account.QuotaRecoveryStatusActive {
			if allowQuotaProbe && quotaRecovery.NextProbeAt != nil && !now.Before(*quotaRecovery.NextProbeAt) {
				probeCandidates = append(probeCandidates, index)
			} else {
				quotaCandidates++
				if quotaRecovery.NextProbeAt != nil {
					earliestRetry = earlierFuture(earliestRetry, *quotaRecovery.NextProbeAt, now)
				}
			}
			continue
		}
		if candidate.Billing != nil && candidate.Billing.IsExhausted(value.MinimumRemaining) {
			quotaCandidates++
			continue
		}
		if candidate.QuotaWindow != nil && candidate.QuotaWindow.Remaining <= 0 {
			quotaCandidates++
			if candidate.QuotaWindow.ResetAt != nil {
				earliestRetry = earlierFuture(earliestRetry, *candidate.QuotaWindow.ResetAt, now)
			}
			continue
		}
		normalCandidates = append(normalCandidates, index)
	}
	if len(normalCandidates) == 0 && len(probeCandidates) == 0 {
		reason := SelectionNoAccounts
		switch {
		case consideredCandidates > 0 && supportedCandidates == 0:
			reason = SelectionUnsupportedModel
		case modelCoolingCandidates > 0:
			reason = SelectionModelCooling
		case coolingCandidates > 0:
			reason = SelectionCooling
		case quotaCandidates > 0:
			reason = SelectionQuotaExhausted
		}
		return nil, &SelectionUnavailableError{Reason: reason, RetryAfter: retryDelay(now, earliestRetry)}
	}
	if len(probeCandidates) > 0 {
		plan, err := s.planCandidateIndexes(ctx, values, probeCandidates, now, s.resolveTierOrder(provider, upstreamModel))
		if err != nil {
			return nil, err
		}
		for candidate, ok := plan.Next(); ok; candidate, ok = plan.Next() {
			lease, err := s.claimAccountSlot(ctx, candidate.Credential)
			if err != nil {
				return nil, err
			}
			if lease == nil {
				continue
			}
			claimed, err := s.accounts.ClaimQuotaProbe(ctx, candidate.Credential.ID, now, now.Add(quotaProbeLease))
			if err != nil || !claimed {
				lease.Release()
				if err != nil {
					return nil, err
				}
				continue
			}
			lease.QuotaProbe = true
			lease.QuotaProbeKind = candidate.QuotaRecovery.Kind
			lease.Billing = candidate.Billing
			return lease, nil
		}
	}
	var saturatedStickyID uint64
	if stickyKey != "" {
		stickyID, ok, err := s.sticky.Get(ctx, stickyKey, now)
		if err != nil {
			return nil, fmt.Errorf("读取会话粘滞状态: %w", err)
		}
		if ok {
			candidate, eligible := routingCandidateByID(values, normalCandidates, stickyID)
			if eligible {
				stickyTTL, _, _, _ := s.routingConfig()
				boundID, bindErr := s.sticky.Bind(ctx, stickyKey, stickyID, now, now.Add(stickyTTL))
				if bindErr != nil {
					return nil, fmt.Errorf("刷新会话粘滞状态: %w", bindErr)
				}
				if boundID != stickyID {
					candidate, eligible = routingCandidateByID(values, normalCandidates, boundID)
					stickyID = boundID
				}
				if eligible {
					lease, acquireErr := s.acquirePinnedCapacity(ctx, candidate.Credential)
					if acquireErr == nil {
						lease.Billing = candidate.Billing
						lease.QuotaMode = effectiveQuotaMode(candidate, quotaMode)
						return lease, nil
					}
					if !isSelectionUnavailable(acquireErr, SelectionSaturated) {
						return nil, acquireErr
					}
					saturatedStickyID = stickyID
				}
			}
		}
	}
	// 粘性账号仅因并发满载而暂时不可用时，先等待该账号；超时后允许本次请求临时借用
	// 其他账号，但不覆盖原绑定，避免并行请求让活跃会话在账号池中来回抖动。
	if saturatedStickyID != 0 {
		plan, err := s.planCandidateIndexes(ctx, values, normalCandidates, time.Now().UTC(), s.resolveTierOrder(provider, upstreamModel))
		if err != nil {
			return nil, err
		}
		for candidate, ok := plan.Next(); ok; candidate, ok = plan.Next() {
			if candidate.Credential.ID == saturatedStickyID {
				continue
			}
			lease, claimErr := s.claimAccountSlot(ctx, candidate.Credential)
			if claimErr != nil {
				return nil, claimErr
			}
			if lease == nil {
				continue
			}
			lease.Billing = candidate.Billing
			lease.QuotaMode = effectiveQuotaMode(candidate, quotaMode)
			return lease, nil
		}
		return nil, &SelectionUnavailableError{Reason: SelectionSaturated, RetryAfter: time.Second}
	}
	if stickyKey == "" {
		activeRequest := s.nextSegmentedActiveRequest(provider, upstreamModel, quotaMode, len(normalCandidates))
		if activeRequest != nil {
			return s.acquireSegmentedCandidates(ctx, values, normalCandidates, quotaMode, s.resolveTierOrder(provider, upstreamModel), *activeRequest)
		}
	}
	_, _, _, capacityWait := s.routingConfig()
	waitDeadline := time.Now().Add(capacityWait)
	for {
		currentTime := time.Now().UTC()
		plan, err := s.planCandidateIndexes(ctx, values, normalCandidates, currentTime, s.resolveTierOrder(provider, upstreamModel))
		if err != nil {
			return nil, err
		}
		for candidate, ok := plan.Next(); ok; candidate, ok = plan.Next() {
			lease, err := s.claimAccountSlot(ctx, candidate.Credential)
			if err != nil {
				return nil, err
			}
			if lease == nil {
				continue
			}
			if stickyKey != "" {
				stickyTTL, _, _, _ := s.routingConfig()
				boundID, bindErr := s.sticky.Bind(ctx, stickyKey, candidate.Credential.ID, currentTime, currentTime.Add(stickyTTL))
				if bindErr != nil {
					lease.Release()
					return nil, fmt.Errorf("写入会话粘滞状态: %w", bindErr)
				}
				if boundID != candidate.Credential.ID {
					if boundCandidate, eligible := routingCandidateByID(values, normalCandidates, boundID); eligible {
						boundLease, boundErr := s.acquirePinnedCapacity(ctx, boundCandidate.Credential)
						if boundErr == nil {
							lease.Release()
							boundLease.Billing = boundCandidate.Billing
							boundLease.QuotaMode = effectiveQuotaMode(boundCandidate, quotaMode)
							return boundLease, nil
						}
						if !isSelectionUnavailable(boundErr, SelectionSaturated) {
							lease.Release()
							return nil, boundErr
						}
						// 已绑定账号满载时保留原绑定，本次请求使用已获取的临时账号。
					} else if err := s.sticky.Set(ctx, stickyKey, candidate.Credential.ID, currentTime.Add(stickyTTL)); err != nil {
						lease.Release()
						return nil, fmt.Errorf("重建会话粘滞状态: %w", err)
					}
				}
			}
			lease.Billing = candidate.Billing
			lease.QuotaMode = effectiveQuotaMode(candidate, quotaMode)
			return lease, nil
		}
		if capacityWait <= 0 {
			return nil, &SelectionUnavailableError{Reason: SelectionSaturated, RetryAfter: time.Second}
		}
		retry, err := s.awaitLeaseRetry(ctx, waitDeadline)
		if err != nil {
			return nil, err
		}
		if !retry {
			return nil, &SelectionUnavailableError{Reason: SelectionSaturated, RetryAfter: time.Second}
		}
	}
}

// stickySessionKey 将调用方粘滞 identity 压缩为固定长度，仅用于账号粘滞索引。
func stickySessionKey(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func routingCandidateByID(values []account.RoutingCandidate, indexes []int, accountID uint64) (account.RoutingCandidate, bool) {
	for _, index := range indexes {
		candidate := values[index]
		if candidate.Credential.ID == accountID {
			return candidate, true
		}
	}
	return account.RoutingCandidate{}, false
}

func isSelectionUnavailable(err error, reason SelectionUnavailableReason) bool {
	var unavailable *SelectionUnavailableError
	return errors.As(err, &unavailable) && unavailable.Reason == reason
}

// AcquirePinned 为 previous_response_id 等账号归属请求获取指定账号租约。
// 优先点查单账号投影，避免加载整个 enabled 池。
func (s *Selector) AcquirePinned(ctx context.Context, provider account.Provider, accountID, modelRouteID uint64, upstreamModel, quotaMode string, inference bool) (*accountLease, error) {
	now := time.Now().UTC()
	candidate, err := s.loadPinnedCandidate(ctx, provider, accountID, modelRouteID, upstreamModel, quotaMode)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, &SelectionUnavailableError{Reason: SelectionNoAccounts}
		}
		return nil, err
	}
	value := candidate.Credential
	if value.ID != accountID || value.Provider != provider {
		return nil, &SelectionUnavailableError{Reason: SelectionNoAccounts}
	}
	if !value.Enabled || value.AuthStatus != account.AuthStatusActive {
		return nil, &SelectionUnavailableError{Reason: SelectionNoAccounts}
	}
	if inference {
		if candidate.ModelCapabilityKnown && !candidate.SupportsModel {
			return nil, &SelectionUnavailableError{Reason: SelectionUnsupportedModel}
		}
		if candidate.ModelQuotaBlock != nil && now.Before(candidate.ModelQuotaBlock.CooldownUntil) {
			return nil, &SelectionUnavailableError{Reason: SelectionModelCooling, RetryAfter: retryDelay(now, candidate.ModelQuotaBlock.CooldownUntil)}
		}
		if value.CooldownUntil != nil && now.Before(*value.CooldownUntil) {
			return nil, &SelectionUnavailableError{Reason: SelectionCooling, RetryAfter: retryDelay(now, *value.CooldownUntil)}
		}
		if recovery := candidate.QuotaRecovery; recovery != nil && recovery.Status != account.QuotaRecoveryStatusActive {
			if recovery.NextProbeAt == nil || now.Before(*recovery.NextProbeAt) {
				var retryAfter time.Duration
				if recovery.NextProbeAt != nil {
					retryAfter = retryDelay(now, *recovery.NextProbeAt)
				}
				return nil, &SelectionUnavailableError{Reason: SelectionQuotaExhausted, RetryAfter: retryAfter}
			}
			lease, err := s.acquirePinnedCapacity(ctx, value)
			if err != nil {
				return nil, err
			}
			claimed, err := s.accounts.ClaimQuotaProbe(ctx, value.ID, now, now.Add(quotaProbeLease))
			if err != nil || !claimed {
				lease.Release()
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("绑定的上游账号恢复探测已被占用")
			}
			lease.QuotaProbe = true
			lease.QuotaProbeKind = recovery.Kind
			lease.Billing = candidate.Billing
			return lease, nil
		}
		if candidate.Billing != nil && candidate.Billing.IsExhausted(value.MinimumRemaining) {
			return nil, &SelectionUnavailableError{Reason: SelectionQuotaExhausted}
		}
		if candidate.QuotaWindow != nil && candidate.QuotaWindow.Remaining <= 0 {
			var retryAfter time.Duration
			if candidate.QuotaWindow.ResetAt != nil {
				retryAfter = retryDelay(now, *candidate.QuotaWindow.ResetAt)
			}
			return nil, &SelectionUnavailableError{Reason: SelectionQuotaExhausted, RetryAfter: retryAfter}
		}
	}
	lease, err := s.acquirePinnedCapacity(ctx, value)
	if err != nil {
		return nil, err
	}
	lease.Billing = candidate.Billing
	lease.QuotaMode = effectiveQuotaMode(candidate, quotaMode)
	return lease, nil
}

func (s *Selector) loadPinnedCandidate(ctx context.Context, provider account.Provider, accountID, modelRouteID uint64, upstreamModel, quotaMode string) (account.RoutingCandidate, error) {
	if lookup, ok := s.accounts.(repository.RoutingAccountLookup); ok {
		candidate, err := lookup.GetRoutingCandidate(ctx, accountID, modelRouteID, upstreamModel, quotaMode)
		if err != nil {
			return account.RoutingCandidate{}, err
		}
		if candidate.Credential.Provider != provider {
			return account.RoutingCandidate{}, repository.ErrNotFound
		}
		return candidate, nil
	}
	// Fallback for fakes without point lookup: scan cached/assembled candidates only after a pool load.
	values, err := s.loadCandidates(ctx, provider, modelRouteID, upstreamModel, quotaMode, time.Now().UTC())
	if err != nil {
		return account.RoutingCandidate{}, err
	}
	for _, candidate := range values {
		if candidate.Credential.ID == accountID {
			return candidate, nil
		}
	}
	return account.RoutingCandidate{}, repository.ErrNotFound
}

func effectiveQuotaMode(candidate account.RoutingCandidate, fallback string) string {
	if candidate.QuotaWindow != nil && candidate.QuotaWindow.Mode == "weekly" {
		return "weekly"
	}
	return fallback
}

func (s *Selector) MarkSuccess(ctx context.Context, credential account.Credential) {
	s.markSuccess(ctx, credential, true)
}

func (s *Selector) markSuccess(ctx context.Context, credential account.Credential, quotaProbe bool) {
	now := time.Now().UTC()
	persist := credential.FailureCount > 0 || credential.CooldownUntil != nil || credential.LastError != ""
	s.selectionMu.Lock()
	if last := s.lastSuccessAt[credential.ID]; last.IsZero() || now.Sub(last) >= successPersistInterval {
		persist = true
	}
	if persist {
		s.lastSuccessAt[credential.ID] = now
	}
	s.selectionMu.Unlock()
	if persist {
		_ = s.accounts.UpdateHealth(ctx, credential.ID, 0, nil, "", true)
	}
	if quotaProbe {
		_ = s.accounts.ClearQuotaRecovery(ctx, credential.ID)
	}
	if quotaProbe || credential.FailureCount > 0 || credential.CooldownUntil != nil || credential.LastError != "" {
		s.invalidateAccount(credential.Provider, credential.ID)
	}
}

func (s *Selector) MarkFreeQuotaExhausted(ctx context.Context, credential account.Credential, used, limit int64) {
	now := time.Now().UTC()
	nextProbeAt := now.Add(defaultFreeQuotaRecoveryPause)
	s.markFreeQuotaExhaustedAt(ctx, credential, used, limit, now, nextProbeAt)
}

func (s *Selector) markFreeQuotaExhaustedAt(ctx context.Context, credential account.Credential, used, limit int64, now, nextProbeAt time.Time) {
	_ = s.accounts.SaveQuotaRecovery(ctx, account.QuotaRecovery{
		AccountID: credential.ID, Kind: account.QuotaRecoveryKindFree, Status: account.QuotaRecoveryStatusExhausted,
		ConfirmedUsed: used, ConfirmedLimit: limit, ExhaustedAt: &now,
		NextProbeAt: &nextProbeAt, LastConfirmedAt: &now, UpdatedAt: now,
	})
	_ = s.sticky.DeleteByAccount(ctx, credential.ID)
	s.invalidateAccount(credential.Provider, credential.ID)
}

func (s *Selector) MarkModelQuotaExhausted(ctx context.Context, credential account.Credential, billing *account.Billing, upstreamModel string, retryAfter time.Duration) {
	upstreamModel = strings.TrimSpace(upstreamModel)
	if upstreamModel == "" {
		s.MarkFreeQuotaExhausted(ctx, credential, 0, 0)
		return
	}
	knownFreeBuild := (account.RoutingCandidate{Credential: credential, Billing: billing}).IsKnownFreeBuild()
	if knownFreeBuild || retryAfter <= 0 {
		retryAfter = defaultFreeQuotaRecoveryPause
	}
	until := time.Now().UTC().Add(retryAfter)
	_ = s.accounts.UpsertModelQuotaBlock(ctx, account.ModelQuotaBlock{
		AccountID: credential.ID, UpstreamModel: upstreamModel, Reason: "model_quota_depleted", CooldownUntil: until, UpdatedAt: time.Now().UTC(),
	})
	// Model quota blocks are overlay-scoped; still account-known so drop assembled without bulk base clear.
	s.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountModelQuotaChanged, Provider: credential.Provider, AccountID: credential.ID, UpstreamModel: upstreamModel,
	})
}

// MarkModelAccessDenied isolates a permission failure to the rejected model.
// Build OAuth accounts may still have valid video access when a chat endpoint
// returns 403, so a model denial must not invalidate the whole credential.
func (s *Selector) MarkModelAccessDenied(ctx context.Context, credential account.Credential, upstreamModel string, retryAfter time.Duration) {
	upstreamModel = strings.TrimSpace(upstreamModel)
	if upstreamModel == "" {
		return
	}
	if retryAfter <= 0 {
		retryAfter = modelAccessDeniedCooldown
	}
	now := time.Now().UTC()
	_ = s.accounts.UpsertModelQuotaBlock(ctx, account.ModelQuotaBlock{
		AccountID: credential.ID, UpstreamModel: upstreamModel, Reason: "model_access_denied",
		CooldownUntil: now.Add(retryAfter), UpdatedAt: now,
	})
	s.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountModelQuotaChanged, Provider: credential.Provider, AccountID: credential.ID, UpstreamModel: upstreamModel,
	})
}

// MarkPaymentQuotaExhausted removes a spending-limited account from routing.
// Paid accounts follow their upstream billing period; Free or unknown accounts
// use the fixed local recovery window.
func (s *Selector) MarkPaymentQuotaExhausted(ctx context.Context, credential account.Credential, hints quotaRecoveryHints) {
	now := time.Now().UTC()
	if hints.Billing != nil && hints.Billing.IsPaid() {
		if periodEnd, ok := hints.Billing.PeriodEnd(); ok && periodEnd.After(now) {
			_ = s.accounts.SaveQuotaRecovery(ctx, account.QuotaRecovery{
				AccountID: credential.ID, Kind: account.QuotaRecoveryKindPaid, Status: account.QuotaRecoveryStatusExhausted,
				ExhaustedAt: &now, NextProbeAt: &periodEnd, LastConfirmedAt: &now, UpdatedAt: now,
			})
			_ = s.sticky.DeleteByAccount(ctx, credential.ID)
			s.invalidateAccount(credential.Provider, credential.ID)
			return
		}
	}
	s.MarkFreeQuotaExhausted(ctx, credential, 0, 0)
}

// MarkQuotaStateChanged 在 Billing 探测改变持久化额度状态后立即失效候选快照。
// 可选 accountID：非 0 时走账号级 base patch，避免热路径 429/billing 抖动整池清 L1。
func (s *Selector) MarkQuotaStateChanged(provider account.Provider, accountID ...uint64) {
	if len(accountID) > 0 && accountID[0] != 0 {
		s.invalidateAccount(provider, accountID[0])
		return
	}
	s.invalidateCandidates(provider)
}

// ConsumeQuota 将成功请求的本地额度变化应用到候选快照，避免为单账号变化清空整个 Provider 缓存。
func (s *Selector) ConsumeQuota(provider account.Provider, accountID uint64, mode string, amount int) {
	if accountID == 0 || mode == "" || mode == "weekly" || amount <= 0 {
		return
	}
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	for key, snapshot := range s.candidates {
		if key.provider != provider {
			continue
		}
		index, found := snapshot.byAccount[accountID]
		if !found || index >= len(snapshot.values) {
			continue
		}
		candidate := snapshot.values[index]
		if candidate.QuotaWindow == nil || candidate.QuotaWindow.Mode != mode {
			continue
		}
		next := append([]account.RoutingCandidate(nil), snapshot.values...)
		window := *next[index].QuotaWindow
		window.Remaining = max(0, window.Remaining-amount)
		window.UpdatedAt = time.Now().UTC()
		next[index].QuotaWindow = &window
		snapshot.values = next
		s.candidates[key] = snapshot
	}
	for key, snapshot := range s.routingBases {
		if key.provider != provider {
			continue
		}
		index := -1
		for candidateIndex, base := range snapshot.values {
			if base.Credential.ID == accountID {
				index = candidateIndex
				break
			}
		}
		if index < 0 || snapshot.values[index].QuotaWindow == nil || snapshot.values[index].QuotaWindow.Mode != mode {
			continue
		}
		next := append([]account.RoutingAccountBase(nil), snapshot.values...)
		window := *next[index].QuotaWindow
		window.Remaining = max(0, window.Remaining-amount)
		window.UpdatedAt = time.Now().UTC()
		next[index].QuotaWindow = &window
		snapshot.values = next
		s.routingBases[key] = snapshot
	}
}

func (s *Selector) MarkFailure(ctx context.Context, credential account.Credential, status int, retryAfter time.Duration) {
	failureCount := credential.FailureCount + 1
	_, cooldownBase, cooldownMax, _ := s.routingConfig()
	cooldown := cooldownBase
	for i := 1; i < failureCount && cooldown < cooldownMax; i++ {
		cooldown *= 2
	}
	if cooldown > cooldownMax {
		cooldown = cooldownMax
	}
	if retryAfter > cooldown {
		cooldown = retryAfter
	}
	until := time.Now().UTC().Add(cooldown)
	// Persist health first. UpdateHealth notifies on failure (with Provider when known); that
	// may already patch L1 via the invalidation observer. Always re-apply with the known
	// provider so cooldown is visible even without an observer and when the notify omits Provider.
	_ = s.accounts.UpdateHealth(ctx, credential.ID, failureCount, &until, fmt.Sprintf("upstream status %d", status), false)
	s.invalidateAccount(credential.Provider, credential.ID)
	if status == 401 || status == 402 || status == 403 || status == 429 {
		_ = s.sticky.DeleteByAccount(ctx, credential.ID)
	}
}

func (s *Selector) loadCandidates(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel, quotaMode string, now time.Time) ([]account.RoutingCandidate, error) {
	if _, ok := s.accounts.(repository.RoutingLayerRepository); ok {
		return s.loadLayeredCandidates(ctx, provider, modelRouteID, upstreamModel, quotaMode, now)
	}
	return s.loadCombinedCandidates(ctx, provider, modelRouteID, upstreamModel, quotaMode, now)
}

func (s *Selector) loadCombinedCandidates(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel, quotaMode string, now time.Time) ([]account.RoutingCandidate, error) {
	key := candidateCacheKey{provider: provider, modelRouteID: modelRouteID, upstreamModel: upstreamModel, quotaMode: quotaMode}
	baseGen := s.routingBaseVersion(provider)
	overlayGen := s.routingOverlayVersion(provider)
	s.candidateMu.Lock()
	if snapshot, ok := s.candidates[key]; ok && s.candidateSnapshotFreshLocked(snapshot, baseGen, overlayGen, now) {
		values := snapshot.values
		s.candidateMu.Unlock()
		s.cacheStats.assembledHits.Add(1)
		return values, nil
	}
	s.candidateMu.Unlock()
	s.cacheStats.assembledMisses.Add(1)
	loadKey := fmt.Sprintf("%s\x00%d\x00%s\x00%s", provider, modelRouteID, upstreamModel, quotaMode)
	loaded, err, _ := s.candidateLoads.Do(loadKey, func() (any, error) {
		checkTime := time.Now().UTC()
		checkBase := s.routingBaseVersion(provider)
		checkOverlay := s.routingOverlayVersion(provider)
		s.candidateMu.Lock()
		if snapshot, ok := s.candidates[key]; ok && s.candidateSnapshotFreshLocked(snapshot, checkBase, checkOverlay, checkTime) {
			values := snapshot.values
			s.candidateMu.Unlock()
			s.cacheStats.assembledHits.Add(1)
			return values, nil
		}
		s.candidateMu.Unlock()
		s.cacheStats.assembledLoads.Add(1)
		values, err := s.accounts.ListRoutingCandidates(ctx, provider, modelRouteID, upstreamModel, quotaMode)
		if err != nil {
			return nil, err
		}
		s.candidateMu.Lock()
		currentBase := s.routingBaseVersionLocked(provider)
		currentOverlay := s.routingOverlayVersionLocked(provider)
		if currentBase == checkBase && currentOverlay == checkOverlay {
			s.candidates[key] = newCandidateSnapshot(values, checkBase, checkOverlay, checkTime)
		}
		s.candidateMu.Unlock()
		return values, nil
	})
	if err != nil {
		return nil, err
	}
	return loaded.([]account.RoutingCandidate), nil
}

func (s *Selector) loadLayeredCandidates(ctx context.Context, provider account.Provider, modelRouteID uint64, upstreamModel, quotaMode string, now time.Time) ([]account.RoutingCandidate, error) {
	key := candidateCacheKey{provider: provider, modelRouteID: modelRouteID, upstreamModel: upstreamModel, quotaMode: quotaMode}
	baseGen := s.routingBaseVersion(provider)
	overlayGen := s.routingOverlayVersion(provider)
	s.candidateMu.Lock()
	if snapshot, ok := s.candidates[key]; ok && s.candidateSnapshotFreshLocked(snapshot, baseGen, overlayGen, now) {
		values := snapshot.values
		s.candidateMu.Unlock()
		s.cacheStats.assembledHits.Add(1)
		return values, nil
	}
	s.candidateMu.Unlock()
	s.cacheStats.assembledMisses.Add(1)
	loadKey := fmt.Sprintf("assembled\x00%s\x00%d\x00%s\x00%s", provider, modelRouteID, upstreamModel, quotaMode)
	loaded, err, _ := s.candidateLoads.Do(loadKey, func() (any, error) {
		checkTime := time.Now().UTC()
		s.candidateMu.Lock()
		checkBase := s.routingBaseVersionLocked(provider)
		checkOverlay := s.routingOverlayVersionLocked(provider)
		if snapshot, ok := s.candidates[key]; ok && s.candidateSnapshotFreshLocked(snapshot, checkBase, checkOverlay, checkTime) {
			values := snapshot.values
			s.candidateMu.Unlock()
			s.cacheStats.assembledHits.Add(1)
			return values, nil
		}
		s.candidateMu.Unlock()
		layered := s.accounts.(repository.RoutingLayerRepository)
		for attempt := 0; attempt < 4; attempt++ {
			bases, baseVersion, loadErr := s.loadRoutingBases(ctx, layered, provider, quotaMode, checkTime)
			if loadErr != nil {
				return nil, loadErr
			}
			overlay, overlayVersion, loadErr := s.loadRoutingOverlay(ctx, layered, provider, modelRouteID, upstreamModel, checkTime)
			if loadErr != nil {
				return nil, loadErr
			}
			if !s.routingVersionsStable(provider, baseVersion, overlayVersion) {
				checkTime = time.Now().UTC()
				continue
			}
			// Assemble purely from in-memory L1/L2 snapshots when both layers are warm.
			values := assembleRoutingCandidates(provider, bases, overlay)
			s.cacheStats.assembledLoads.Add(1)
			s.candidateMu.Lock()
			stable := baseVersion == s.routingBaseVersionLocked(provider) && overlayVersion == s.routingOverlayVersionLocked(provider)
			if stable {
				s.candidates[key] = newCandidateSnapshot(values, baseVersion, overlayVersion, checkTime)
			}
			s.candidateMu.Unlock()
			if stable {
				return values, nil
			}
			checkTime = time.Now().UTC()
		}
		// Sustained account synchronization must not turn cache churn into user-facing
		// failures. Fall back to the established authoritative combined query.
		s.cacheStats.assembledLoads.Add(1)
		return s.accounts.ListRoutingCandidates(ctx, provider, modelRouteID, upstreamModel, quotaMode)
	})
	if err != nil {
		return nil, err
	}
	return loaded.([]account.RoutingCandidate), nil
}

func (s *Selector) loadRoutingBases(ctx context.Context, layered repository.RoutingLayerRepository, provider account.Provider, quotaMode string, now time.Time) ([]account.RoutingAccountBase, routingLayerVersion, error) {
	key := routingBaseCacheKey{provider: provider, quotaMode: quotaMode}
	version := s.routingBaseVersion(provider)
	s.candidateMu.Lock()
	if snapshot, ok := s.routingBases[key]; ok && s.baseSnapshotFreshLocked(snapshot, version, now) {
		values := snapshot.values
		s.candidateMu.Unlock()
		s.cacheStats.baseHits.Add(1)
		return values, version, nil
	}
	s.candidateMu.Unlock()
	s.cacheStats.baseMisses.Add(1)
	loadKey := "base\x00" + string(provider) + "\x00" + quotaMode
	loaded, err, _ := s.candidateLoads.Do(loadKey, func() (any, error) {
		checkTime := time.Now().UTC()
		checkVersion := s.routingBaseVersion(provider)
		s.candidateMu.Lock()
		if snapshot, ok := s.routingBases[key]; ok && s.baseSnapshotFreshLocked(snapshot, checkVersion, checkTime) {
			values := snapshot.values
			s.candidateMu.Unlock()
			s.cacheStats.baseHits.Add(1)
			return routingBaseLoadResult{values: values, version: checkVersion}, nil
		}
		s.candidateMu.Unlock()
		s.cacheStats.baseLoads.Add(1)
		values, loadErr := layered.ListRoutingAccountBases(ctx, provider, quotaMode)
		if loadErr != nil {
			return nil, loadErr
		}
		s.candidateMu.Lock()
		currentVersion := s.routingBaseVersionLocked(provider)
		if currentVersion == checkVersion {
			s.routingBases[key] = routingBaseSnapshot{values: values, version: checkVersion, loadedAt: checkTime}
		}
		s.candidateMu.Unlock()
		return routingBaseLoadResult{values: values, version: checkVersion}, nil
	})
	if err != nil {
		return nil, routingLayerVersion{}, err
	}
	result := loaded.(routingBaseLoadResult)
	return result.values, result.version, nil
}

func (s *Selector) loadRoutingOverlay(ctx context.Context, layered repository.RoutingLayerRepository, provider account.Provider, modelRouteID uint64, upstreamModel string, now time.Time) (account.RoutingOverlaySnapshot, routingLayerVersion, error) {
	key := routingOverlayCacheKey{provider: provider, modelRouteID: modelRouteID, upstreamModel: upstreamModel}
	version := s.routingOverlayVersion(provider)
	s.candidateMu.Lock()
	if snapshot, ok := s.routingOverlays[key]; ok && s.overlaySnapshotFreshLocked(snapshot, version, now) {
		value := snapshot.value
		s.candidateMu.Unlock()
		s.cacheStats.overlayHits.Add(1)
		return value, version, nil
	}
	s.candidateMu.Unlock()
	s.cacheStats.overlayMisses.Add(1)
	loadKey := fmt.Sprintf("overlay\x00%s\x00%d\x00%s", provider, modelRouteID, upstreamModel)
	loaded, err, _ := s.candidateLoads.Do(loadKey, func() (any, error) {
		checkTime := time.Now().UTC()
		checkVersion := s.routingOverlayVersion(provider)
		s.candidateMu.Lock()
		if snapshot, ok := s.routingOverlays[key]; ok && s.overlaySnapshotFreshLocked(snapshot, checkVersion, checkTime) {
			value := snapshot.value
			s.candidateMu.Unlock()
			s.cacheStats.overlayHits.Add(1)
			return routingOverlayLoadResult{value: value, version: checkVersion}, nil
		}
		s.candidateMu.Unlock()
		s.cacheStats.overlayLoads.Add(1)
		value, loadErr := layered.ListRoutingAccountOverlays(ctx, provider, modelRouteID, upstreamModel)
		if loadErr != nil {
			return nil, loadErr
		}
		s.candidateMu.Lock()
		currentVersion := s.routingOverlayVersionLocked(provider)
		if currentVersion == checkVersion {
			s.routingOverlays[key] = routingOverlaySnapshot{value: value, version: checkVersion, loadedAt: checkTime}
		}
		s.candidateMu.Unlock()
		return routingOverlayLoadResult{value: value, version: checkVersion}, nil
	})
	if err != nil {
		return account.RoutingOverlaySnapshot{}, routingLayerVersion{}, err
	}
	result := loaded.(routingOverlayLoadResult)
	return result.value, result.version, nil
}

func (s *Selector) candidateSnapshotFreshLocked(snapshot candidateSnapshot, baseGen, overlayGen routingLayerVersion, now time.Time) bool {
	if snapshot.baseGen != baseGen || snapshot.overlayGen != overlayGen {
		return false
	}
	if candidateCacheSoftTTL > 0 && !snapshot.loadedAt.IsZero() && now.Sub(snapshot.loadedAt) > candidateCacheSoftTTL {
		return false
	}
	return true
}

func (s *Selector) baseSnapshotFreshLocked(snapshot routingBaseSnapshot, version routingLayerVersion, now time.Time) bool {
	if snapshot.version != version {
		return false
	}
	if candidateCacheSoftTTL > 0 && !snapshot.loadedAt.IsZero() && now.Sub(snapshot.loadedAt) > candidateCacheSoftTTL {
		return false
	}
	return true
}

func (s *Selector) overlaySnapshotFreshLocked(snapshot routingOverlaySnapshot, version routingLayerVersion, now time.Time) bool {
	if snapshot.version != version {
		return false
	}
	if candidateCacheSoftTTL > 0 && !snapshot.loadedAt.IsZero() && now.Sub(snapshot.loadedAt) > candidateCacheSoftTTL {
		return false
	}
	return true
}

func (s *Selector) routingBaseVersion(provider account.Provider) routingLayerVersion {
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	return s.routingBaseVersionLocked(provider)
}

func (s *Selector) routingBaseVersionLocked(provider account.Provider) routingLayerVersion {
	return routingLayerVersion{global: s.baseGlobalVersion, provider: s.baseProviderVersion[provider]}
}

func (s *Selector) routingOverlayVersion(provider account.Provider) routingLayerVersion {
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	return s.routingOverlayVersionLocked(provider)
}

func (s *Selector) routingOverlayVersionLocked(provider account.Provider) routingLayerVersion {
	return routingLayerVersion{global: s.overlayGlobalVersion, provider: s.overlayProviderVersion[provider]}
}

func (s *Selector) routingVersionsStable(provider account.Provider, base, overlay routingLayerVersion) bool {
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	return base == s.routingBaseVersionLocked(provider) && overlay == s.routingOverlayVersionLocked(provider)
}

// ApplyInvalidation advances local layer generations before any remote publish.
// Token-only credential updates do not rebuild the routing base pool: selection
// projections never carry encrypted tokens, and EnsureCredential reloads secrets via Get.
// Single-account base events patch in-memory bases (or drop one account) without clearing
// the whole provider pool; bulk events without AccountID still full-rebuild on next load.
// Model×account overlay events patch one overlay row (+ matching L3) when possible.
func (s *Selector) ApplyInvalidation(event repository.InvalidationEvent) {
	if !event.Valid() {
		return
	}
	if event.Kind == repository.InvalidationAccountCredentialChanged {
		s.cacheStats.tokenOnlySkipped.Add(1)
		return
	}
	base := event.Layer() == repository.InvalidationLayerBase
	overlay := event.Layer() == repository.InvalidationLayerOverlay || event.Layer() == repository.InvalidationLayerRoute

	if base && event.AccountID != 0 {
		s.patchBaseAccount(event)
		return
	}
	if overlay && event.AccountID != 0 && event.UpstreamModel != "" && event.Layer() == repository.InvalidationLayerOverlay {
		s.patchOverlayAccount(event)
		return
	}

	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	if base {
		s.cacheStats.baseEvents.Add(1)
		s.cacheStats.bulkRebuilds.Add(1)
		s.cacheStats.baseRebuilds.Add(1)
		if event.Provider == "" {
			s.baseGlobalVersion++
			clearRoutingBases(s.routingBases, "")
		} else {
			s.baseProviderVersion[event.Provider]++
			clearRoutingBases(s.routingBases, event.Provider)
		}
		for key := range s.candidates {
			if event.Provider == "" || key.provider == event.Provider {
				delete(s.candidates, key)
			}
		}
	}
	if overlay {
		s.cacheStats.overlayEvents.Add(1)
		if event.Provider == "" {
			s.overlayGlobalVersion++
			clearRoutingOverlays(s.routingOverlays, "")
		} else {
			s.overlayProviderVersion[event.Provider]++
			if event.UpstreamModel != "" {
				clearRoutingOverlaysForModel(s.routingOverlays, event.Provider, event.UpstreamModel)
			} else {
				clearRoutingOverlays(s.routingOverlays, event.Provider)
			}
		}
		for key := range s.candidates {
			if event.Provider != "" && key.provider != event.Provider {
				continue
			}
			if event.UpstreamModel != "" && key.upstreamModel != event.UpstreamModel {
				continue
			}
			delete(s.candidates, key)
		}
	}
}

// patchBaseAccount updates warm L1 base snapshots for one account without clearing the pool.
// Point-loads run outside the cache lock when RoutingAccountLookup is available.
//
// Fail-open rules (do not stamp a truncated pool as fresh for soft TTL):
//   - missing RoutingAccountLookup → bulk clear provider bases
//   - GetRoutingAccountBase timeout/error (except ErrNotFound) → bulk clear
//   - ErrNotFound / disabled / non-active → remove that account only
func (s *Selector) patchBaseAccount(event repository.InvalidationEvent) {
	s.cacheStats.baseEvents.Add(1)
	provider := event.Provider
	accountID := event.AccountID

	type modeKey struct {
		provider  account.Provider
		quotaMode string
	}
	s.candidateMu.Lock()
	modes := make([]modeKey, 0, 4)
	seen := make(map[modeKey]struct{})
	for key := range s.routingBases {
		if provider != "" && key.provider != provider {
			continue
		}
		mk := modeKey{provider: key.provider, quotaMode: key.quotaMode}
		if _, ok := seen[mk]; ok {
			continue
		}
		seen[mk] = struct{}{}
		modes = append(modes, mk)
	}
	s.candidateMu.Unlock()

	lookup, hasLookup := s.accounts.(repository.RoutingAccountLookup)
	if !hasLookup {
		// No point API: match pre-Phase-1 safety — clear warm bases so next load full-lists.
		s.bulkRebuildBase(provider)
		return
	}

	// Cold L1: nothing to patch; bump generation and drop assembled so the next load full-lists.
	if len(modes) == 0 {
		s.bulkRebuildBase(provider)
		// bulkRebuildBase already counts rebuild; avoid double-counting baseEvents-only path noise.
		return
	}

	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), routingBasePointLoadTimeout)
	defer cancel()

	replacements := make(map[modeKey]*account.RoutingAccountBase, len(modes))
	for _, mk := range modes {
		base, err := lookup.GetRoutingAccountBase(loadCtx, accountID, mk.quotaMode)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				replacements[mk] = nil // definitive removal
				continue
			}
			// Transient DB error or timeout: fail open to full provider rebuild.
			s.bulkRebuildBase(provider)
			return
		}
		if provider == "" {
			provider = base.Credential.Provider
		}
		if !base.Credential.Enabled || base.Credential.AuthStatus != account.AuthStatusActive {
			replacements[mk] = nil
			continue
		}
		copyBase := base
		replacements[mk] = &copyBase
	}

	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	if provider != "" {
		s.baseProviderVersion[provider]++
	} else {
		s.baseGlobalVersion++
	}

	changed := false
	for key, snap := range s.routingBases {
		if provider != "" && key.provider != provider {
			continue
		}
		mk := modeKey{provider: key.provider, quotaMode: key.quotaMode}
		nextBase, haveReplacement := replacements[mk]
		if !haveReplacement {
			// Mode disappeared between collect and apply; leave this snapshot alone.
			continue
		}
		values := make([]account.RoutingAccountBase, 0, len(snap.values))
		found := false
		for _, base := range snap.values {
			if base.Credential.ID != accountID {
				values = append(values, base)
				continue
			}
			found = true
			if nextBase != nil {
				values = append(values, *nextBase)
			}
		}
		if !found && nextBase != nil {
			values = append(values, *nextBase)
			found = true
		}
		if !found && nextBase == nil {
			// Account was not in this snapshot and should stay absent.
			continue
		}
		snap.values = values
		snap.loadedAt = time.Now().UTC()
		snap.version = s.routingBaseVersionLocked(key.provider)
		s.routingBases[key] = snap
		changed = true
	}
	if changed {
		s.cacheStats.basePatches.Add(1)
	}
	// Patch L3 rows in place instead of deleting every assembled entry for the provider.
	// Prefer the replacement base from any mode key (same credential state across modes).
	var replacement *account.RoutingAccountBase
	for _, next := range replacements {
		if next != nil {
			replacement = next
			break
		}
	}
	// If every mode said remove (nil), drop the account from L3.
	drop := true
	for _, next := range replacements {
		if next != nil {
			drop = false
			break
		}
	}
	// L3 snapshots must adopt the just-bumped base version so they stay fresh;
	// otherwise the version check would treat every patched snapshot as stale.
	nextBaseGen := s.routingBaseVersionLocked(provider)
	if drop {
		if patchAssembledAccountLocked(s.candidates, provider, accountID, nil, nextBaseGen, routingLayerVersion{}) {
			s.cacheStats.assembledPatches.Add(1)
		}
		return
	}
	if patchAssembledAccountLocked(s.candidates, provider, accountID, replacement, nextBaseGen, routingLayerVersion{}) {
		s.cacheStats.assembledPatches.Add(1)
	}
}

// patchOverlayAccount updates one account's overlay row for a known upstream model
// without clearing the whole model snapshot when warm L2/L3 entries exist.
func (s *Selector) patchOverlayAccount(event repository.InvalidationEvent) {
	s.cacheStats.overlayEvents.Add(1)
	provider := event.Provider
	accountID := event.AccountID
	upstreamModel := event.UpstreamModel

	lookup, hasLookup := s.accounts.(repository.RoutingAccountLookup)
	if !hasLookup {
		s.bulkClearOverlayModel(provider, upstreamModel)
		return
	}

	s.candidateMu.Lock()
	overlayKeys := make([]routingOverlayCacheKey, 0, 4)
	for key := range s.routingOverlays {
		if (provider == "" || key.provider == provider) && key.upstreamModel == upstreamModel {
			overlayKeys = append(overlayKeys, key)
		}
	}
	assembledKeys := make([]candidateCacheKey, 0, 4)
	for key := range s.candidates {
		if (provider == "" || key.provider == provider) && key.upstreamModel == upstreamModel {
			assembledKeys = append(assembledKeys, key)
		}
	}
	s.candidateMu.Unlock()

	// Cold L2 and L3: bump overlay generation so the next load reloads from DB.
	if len(overlayKeys) == 0 && len(assembledKeys) == 0 {
		s.candidateMu.Lock()
		if provider == "" {
			s.overlayGlobalVersion++
		} else {
			s.overlayProviderVersion[provider]++
		}
		s.candidateMu.Unlock()
		return
	}

	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), routingBasePointLoadTimeout)
	defer cancel()

	// Collect point-loaded overlay fields per modelRouteID (and quotaMode for L3).
	type overlayPatch struct {
		value account.RoutingAccountOverlay
		drop  bool
	}
	overlayPatches := make(map[routingOverlayCacheKey]overlayPatch, len(overlayKeys))
	for _, key := range overlayKeys {
		candidate, err := lookup.GetRoutingCandidate(loadCtx, accountID, key.modelRouteID, upstreamModel, "")
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				overlayPatches[key] = overlayPatch{drop: true}
				continue
			}
			s.bulkClearOverlayModel(provider, upstreamModel)
			return
		}
		overlayPatches[key] = overlayPatch{value: account.RoutingAccountOverlay{
			AccountID: accountID, Bound: true, ModelCapabilityKnown: candidate.ModelCapabilityKnown,
			SupportsModel: candidate.SupportsModel, ModelQuotaBlock: candidate.ModelQuotaBlock,
		}}
	}

	assembledPatches := make(map[candidateCacheKey]*account.RoutingCandidate, len(assembledKeys))
	for _, key := range assembledKeys {
		candidate, err := lookup.GetRoutingCandidate(loadCtx, accountID, key.modelRouteID, upstreamModel, key.quotaMode)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				assembledPatches[key] = nil
				continue
			}
			s.bulkClearOverlayModel(provider, upstreamModel)
			return
		}
		copyCandidate := candidate
		assembledPatches[key] = &copyCandidate
	}

	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	// Do not bump overlay generation: in-place patch keeps warm snapshots valid.
	changedOverlay := false
	for key, patch := range overlayPatches {
		snap, ok := s.routingOverlays[key]
		if !ok {
			continue
		}
		values := make([]account.RoutingAccountOverlay, 0, len(snap.value.Values))
		found := false
		for _, row := range snap.value.Values {
			if row.AccountID != accountID {
				values = append(values, row)
				continue
			}
			found = true
			if !patch.drop {
				values = append(values, patch.value)
			}
		}
		if !found && !patch.drop {
			values = append(values, patch.value)
			found = true
		}
		if !found && patch.drop {
			continue
		}
		snap.value.Values = values
		snap.loadedAt = time.Now().UTC()
		s.routingOverlays[key] = snap
		changedOverlay = true
	}
	if changedOverlay {
		s.cacheStats.overlayPatches.Add(1)
	}

	changedAssembled := false
	for key, next := range assembledPatches {
		snap, ok := s.candidates[key]
		if !ok {
			continue
		}
		if next == nil {
			if dropAssembledAccountInSnapshot(&snap, accountID) {
				s.candidates[key] = snap
				changedAssembled = true
			}
			continue
		}
		if upsertAssembledAccountInSnapshot(&snap, *next) {
			s.candidates[key] = snap
			changedAssembled = true
		}
	}
	if changedAssembled {
		s.cacheStats.assembledPatches.Add(1)
	}
}

// bulkClearOverlayModel clears warm L2/L3 for a model (fail-open / no lookup).
func (s *Selector) bulkClearOverlayModel(provider account.Provider, upstreamModel string) {
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	if provider == "" {
		s.overlayGlobalVersion++
		clearRoutingOverlays(s.routingOverlays, "")
	} else {
		s.overlayProviderVersion[provider]++
		if upstreamModel != "" {
			clearRoutingOverlaysForModel(s.routingOverlays, provider, upstreamModel)
		} else {
			clearRoutingOverlays(s.routingOverlays, provider)
		}
	}
	for key := range s.candidates {
		if provider != "" && key.provider != provider {
			continue
		}
		if upstreamModel != "" && key.upstreamModel != upstreamModel {
			continue
		}
		delete(s.candidates, key)
	}
}

// patchAssembledAccountLocked updates or drops one account in warm L3 snapshots.
// caller must hold candidateMu. nextBase nil means drop. Overlay fields are preserved
// when updating from a base replacement. baseGen/overlayGen, when non-zero, are adopted
// by patched snapshots so the next freshness check passes without a reload.
func patchAssembledAccountLocked(candidates map[candidateCacheKey]candidateSnapshot, provider account.Provider, accountID uint64, nextBase *account.RoutingAccountBase, baseGen, overlayGen routingLayerVersion) bool {
	changed := false
	now := time.Now().UTC()
	for key, snap := range candidates {
		if provider != "" && key.provider != provider {
			continue
		}
		if nextBase == nil {
			if dropAssembledAccountInSnapshot(&snap, accountID) {
				// Drop keeps overlayGen; refresh baseGen so the shrunk snapshot stays valid.
				if baseGen != (routingLayerVersion{}) {
					snap.baseGen = baseGen
				}
				snap.loadedAt = now
				candidates[key] = snap
				changed = true
			}
			continue
		}
		index, found := snap.byAccount[accountID]
		if !found || index >= len(snap.values) {
			// Account not in this model assembled list — leave alone (may be unbound).
			continue
		}
		next := append([]account.RoutingCandidate(nil), snap.values...)
		candidate := next[index]
		candidate.Credential = nextBase.Credential
		candidate.Billing = nextBase.Billing
		candidate.QuotaWindow = nextBase.QuotaWindow
		candidate.QuotaRecovery = nextBase.QuotaRecovery
		next[index] = candidate
		snap.values = next
		if baseGen != (routingLayerVersion{}) {
			snap.baseGen = baseGen
		}
		snap.loadedAt = now
		// byAccount indexes unchanged
		candidates[key] = snap
		changed = true
	}
	return changed
}

func dropAssembledAccountInSnapshot(snap *candidateSnapshot, accountID uint64) bool {
	index, found := snap.byAccount[accountID]
	if !found || index >= len(snap.values) {
		return false
	}
	next := make([]account.RoutingCandidate, 0, len(snap.values)-1)
	next = append(next, snap.values[:index]...)
	next = append(next, snap.values[index+1:]...)
	*snap = newCandidateSnapshot(next, snap.baseGen, snap.overlayGen, snap.loadedAt)
	return true
}

func upsertAssembledAccountInSnapshot(snap *candidateSnapshot, candidate account.RoutingCandidate) bool {
	index, found := snap.byAccount[candidate.Credential.ID]
	if found && index < len(snap.values) {
		next := append([]account.RoutingCandidate(nil), snap.values...)
		next[index] = candidate
		snap.values = next
		return true
	}
	next := append(append([]account.RoutingCandidate(nil), snap.values...), candidate)
	*snap = newCandidateSnapshot(next, snap.baseGen, snap.overlayGen, time.Now().UTC())
	return true
}

// bulkRebuildBase clears warm L1 bases (and assembled) for a provider so the next load full-lists.
func (s *Selector) bulkRebuildBase(provider account.Provider) {
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()
	s.cacheStats.bulkRebuilds.Add(1)
	s.cacheStats.baseRebuilds.Add(1)
	if provider == "" {
		s.baseGlobalVersion++
		clearRoutingBases(s.routingBases, "")
	} else {
		s.baseProviderVersion[provider]++
		clearRoutingBases(s.routingBases, provider)
	}
	for key := range s.candidates {
		if provider == "" || key.provider == provider {
			delete(s.candidates, key)
		}
	}
}

func clearRoutingBases(values map[routingBaseCacheKey]routingBaseSnapshot, provider account.Provider) {
	for key := range values {
		if provider == "" || key.provider == provider {
			delete(values, key)
		}
	}
}

func clearRoutingOverlays(values map[routingOverlayCacheKey]routingOverlaySnapshot, provider account.Provider) {
	for key := range values {
		if provider == "" || key.provider == provider {
			delete(values, key)
		}
	}
}

func clearRoutingOverlaysForModel(values map[routingOverlayCacheKey]routingOverlaySnapshot, provider account.Provider, upstreamModel string) {
	for key := range values {
		if (provider == "" || key.provider == provider) && key.upstreamModel == upstreamModel {
			delete(values, key)
		}
	}
}

type routingBaseLoadResult struct {
	values  []account.RoutingAccountBase
	version routingLayerVersion
}

type routingOverlayLoadResult struct {
	value   account.RoutingOverlaySnapshot
	version routingLayerVersion
}

func assembleRoutingCandidates(provider account.Provider, bases []account.RoutingAccountBase, overlay account.RoutingOverlaySnapshot) []account.RoutingCandidate {
	byAccount := make(map[uint64]account.RoutingAccountOverlay, len(overlay.Values))
	for _, value := range overlay.Values {
		byAccount[value.AccountID] = value
	}
	sharedSuperBuildModel := false
	if provider == account.ProviderBuild && !overlay.HasBindings {
		for _, base := range bases {
			value, exists := byAccount[base.Credential.ID]
			if exists && value.SupportsModel && account.IsBuildSuper(base.Credential, base.Billing) {
				sharedSuperBuildModel = true
				break
			}
		}
	}
	result := make([]account.RoutingCandidate, 0, len(bases))
	for _, base := range bases {
		overlayValue := byAccount[base.Credential.ID]
		if overlay.HasBindings && !overlayValue.Bound {
			continue
		}
		known, supports := overlayValue.ModelCapabilityKnown, overlayValue.SupportsModel
		if overlay.HasBindings {
			known, supports = true, true
		} else if sharedSuperBuildModel && account.IsBuildSuper(base.Credential, base.Billing) {
			known, supports = true, true
		}
		result = append(result, account.RoutingCandidate{
			Credential: base.Credential, Billing: base.Billing, QuotaWindow: base.QuotaWindow, QuotaRecovery: base.QuotaRecovery,
			ModelQuotaBlock: overlayValue.ModelQuotaBlock, ModelCapabilityKnown: known, SupportsModel: supports,
		})
	}
	return result
}

func (s *Selector) invalidateCandidates(provider account.Provider) {
	s.ApplyInvalidation(repository.InvalidationEvent{Kind: repository.InvalidationAccountStateChanged, Provider: provider})
	s.ApplyInvalidation(repository.InvalidationEvent{Kind: repository.InvalidationAccountCapabilityChanged, Provider: provider})
}

// invalidateAccount applies account-scoped base invalidation so warm L1 pools can be patched
// instead of bulk-cleared when the caller knows which account changed.
func (s *Selector) invalidateAccount(provider account.Provider, accountID uint64) {
	if accountID == 0 {
		s.invalidateCandidates(provider)
		return
	}
	s.ApplyInvalidation(repository.InvalidationEvent{
		Kind: repository.InvalidationAccountStateChanged, Provider: provider, AccountID: accountID,
	})
}

func (s *Selector) claimAccountSlot(ctx context.Context, value account.Credential) (*accountLease, error) {
	limit := value.MaxConcurrent
	if limit <= 0 {
		limit = account.DefaultMaxConcurrent
	}
	release, acquired, err := s.concurrency.Acquire(ctx, accountConcurrencyKey(value.ID), limit)
	if err != nil {
		return nil, fmt.Errorf("获取账号并发租约: %w", err)
	}
	if !acquired {
		return nil, nil
	}
	s.selectionMu.Lock()
	s.lastSelectedAt[value.ID] = time.Now().UTC()
	s.selectionMu.Unlock()
	return &accountLease{Credential: value, release: func() {
		release()
		s.announceLeaseReturn()
	}}, nil
}

func (s *Selector) acquirePinnedCapacity(ctx context.Context, value account.Credential) (*accountLease, error) {
	_, _, _, capacityWait := s.routingConfig()
	deadline := time.Now().Add(capacityWait)
	for {
		lease, err := s.claimAccountSlot(ctx, value)
		if err != nil || lease != nil {
			return lease, err
		}
		if capacityWait <= 0 {
			return nil, &SelectionUnavailableError{Reason: SelectionSaturated, RetryAfter: time.Second}
		}
		retry, err := s.awaitLeaseRetry(ctx, deadline)
		if err != nil {
			return nil, err
		}
		if !retry {
			return nil, &SelectionUnavailableError{Reason: SelectionSaturated, RetryAfter: time.Second}
		}
	}
}

func (s *Selector) leaseReturnNotice() <-chan struct{} {
	s.leaseWakeMu.Lock()
	defer s.leaseWakeMu.Unlock()
	if s.leaseWake == nil {
		s.leaseWake = make(chan struct{})
	}
	return s.leaseWake
}

func (s *Selector) announceLeaseReturn() {
	s.leaseWakeMu.Lock()
	if s.leaseWake != nil {
		close(s.leaseWake)
	}
	s.leaseWake = make(chan struct{})
	s.leaseWakeMu.Unlock()
}

// awaitLeaseRetry 在本实例归还租约时立即重试；短轮询用于感知其他实例释放的共享并发名额。
func (s *Selector) awaitLeaseRetry(ctx context.Context, deadline time.Time) (bool, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false, nil
	}
	notice := s.leaseReturnNotice()
	timer := time.NewTimer(min(remaining, 100*time.Millisecond))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-notice:
		return true, nil
	case <-timer.C:
		return time.Now().Before(deadline), nil
	}
}

func earlierFuture(current, candidate, now time.Time) time.Time {
	if candidate.IsZero() || !now.Before(candidate) {
		return current
	}
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

func retryDelay(now, retryAt time.Time) time.Duration {
	if retryAt.IsZero() || !now.Before(retryAt) {
		return 0
	}
	return retryAt.Sub(now)
}

func (s *Selector) resolveTierOrder(provider account.Provider, upstreamModel string) []account.WebTier {
	if s.tierOrders == nil {
		return nil
	}
	return s.tierOrders.TierOrder(provider, upstreamModel)
}

func tierOrderRank(order []account.WebTier, tier account.WebTier) int {
	for index, value := range order {
		if value == tier {
			return index
		}
	}
	return len(order)
}
