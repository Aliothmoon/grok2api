package gateway

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

type candidateScore struct {
	index           int
	tier            int
	preferFreeBuild bool
	billingFresh    bool
	inFlight        int
	remaining       float64
	lastSelected    time.Time
}

// candidatePlan 使用线性建堆保留完整路由优先级，并允许 claim 失败后按顺序取下一账号。
type candidatePlan struct {
	values []account.RoutingCandidate
	scores []candidateScore
}

func (p *candidatePlan) Len() int { return len(p.scores) }

func (p *candidatePlan) Less(left, right int) bool {
	return candidateScoreBetter(p.values, p.scores[left], p.scores[right])
}

func (p *candidatePlan) Swap(left, right int) {
	p.scores[left], p.scores[right] = p.scores[right], p.scores[left]
}

func (p *candidatePlan) Push(value any) {
	p.scores = append(p.scores, value.(candidateScore))
}

func (p *candidatePlan) Pop() any {
	last := len(p.scores) - 1
	value := p.scores[last]
	p.scores = p.scores[:last]
	return value
}

func (p *candidatePlan) Next() (account.RoutingCandidate, bool) {
	if p == nil || p.Len() == 0 {
		return account.RoutingCandidate{}, false
	}
	score := heap.Pop(p).(candidateScore)
	return p.values[score.index], true
}

func candidateScoreBetter(values []account.RoutingCandidate, leftScore, rightScore candidateScore) bool {
	leftCandidate, rightCandidate := values[leftScore.index], values[rightScore.index]
	left, right := leftCandidate.Credential, rightCandidate.Credential
	if leftCandidate.SupportsModel != rightCandidate.SupportsModel {
		return leftCandidate.SupportsModel
	}
	if leftCandidate.ModelCapabilityKnown != rightCandidate.ModelCapabilityKnown {
		return leftCandidate.ModelCapabilityKnown
	}
	if leftScore.preferFreeBuild != rightScore.preferFreeBuild {
		return leftScore.preferFreeBuild
	}
	if leftScore.tier != rightScore.tier {
		return leftScore.tier < rightScore.tier
	}
	if left.Priority != right.Priority {
		return left.Priority > right.Priority
	}
	if leftScore.billingFresh != rightScore.billingFresh {
		return leftScore.billingFresh
	}
	if leftScore.inFlight != rightScore.inFlight {
		return leftScore.inFlight < rightScore.inFlight
	}
	if leftScore.remaining != rightScore.remaining {
		return leftScore.remaining > rightScore.remaining
	}
	if !leftScore.lastSelected.Equal(rightScore.lastSelected) {
		return leftScore.lastSelected.Before(rightScore.lastSelected)
	}
	return left.ID < right.ID
}

// planCandidates 批量读取动态并发状态，并以 O(n) 建堆生成保持原比较规则的候选计划。
func (s *Selector) planCandidates(ctx context.Context, values []account.RoutingCandidate, now time.Time, tierOrder []account.WebTier) (*candidatePlan, error) {
	return s.planCandidateIndexes(ctx, values, nil, now, tierOrder)
}

// planCandidateIndexes 在不可变候选快照上按下标规划，避免过滤阶段复制完整账号结构。
// indexes 为 nil 时表示使用 values 的全部元素。
func (s *Selector) planCandidateIndexes(ctx context.Context, values []account.RoutingCandidate, indexes []int, now time.Time, tierOrder []account.WebTier) (*candidatePlan, error) {
	return s.planCandidateIndexesWithHints(ctx, values, indexes, now, tierOrder, nil, s.preferFreeBuildEnabled())
}

func (s *Selector) planCandidateIndexesWithHints(ctx context.Context, values []account.RoutingCandidate, indexes []int, now time.Time, tierOrder []account.WebTier, concurrencyHints []int, preferFreeBuild bool) (*candidatePlan, error) {
	length := len(indexes)
	if indexes == nil {
		length = len(values)
	}
	inFlight := make([]int, length)
	if concurrencyHints == nil {
		ids := make([]uint64, length)
		for position := range length {
			index := position
			if indexes != nil {
				index = indexes[position]
			}
			ids[position] = values[index].Credential.ID
		}
		concurrencySnapshot, err := s.loadConcurrencySnapshotByAccountIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		for position := range length {
			// Sparse map: missing id ⇒ 0.
			inFlight[position] = concurrencySnapshot[ids[position]]
		}
	} else {
		missingIndexes := make([]int, 0, length)
		ids := make([]uint64, 0, length)
		for position := range length {
			index := position
			if indexes != nil {
				index = indexes[position]
			}
			if concurrencyHints[index] != 0 {
				continue
			}
			missingIndexes = append(missingIndexes, index)
			ids = append(ids, values[index].Credential.ID)
		}
		if len(ids) > 0 {
			concurrencySnapshot, err := s.loadConcurrencySnapshotByAccountIDs(ctx, ids)
			if err != nil {
				return nil, err
			}
			for position, index := range missingIndexes {
				// Sparse map: missing id ⇒ 0; store count+1 so 0 occupancy is distinguishable from unset.
				concurrencyHints[index] = concurrencySnapshot[ids[position]] + 1
			}
		}
		for position := range length {
			index := position
			if indexes != nil {
				index = indexes[position]
			}
			inFlight[position] = concurrencyHints[index] - 1
		}
	}

	s.selectionMu.RLock()
	scores := make([]candidateScore, 0, length)
	for position := range length {
		index := position
		if indexes != nil {
			index = indexes[position]
		}
		candidate := values[index]
		limit := candidate.Credential.MaxConcurrent
		if limit <= 0 {
			limit = account.DefaultMaxConcurrent
		}
		// 已知满载的账号不进入计划，避免高优先级满载账号逐个 claim 失败后
		// 才轮到仍有容量的低优先级账号。
		if inFlight[position] >= limit {
			continue
		}
		score := candidateScore{
			index: index, tier: tierOrderRank(tierOrder, candidate.Credential.WebTier),
			preferFreeBuild: preferFreeBuild && candidate.IsKnownFreeBuild(),
			inFlight:        inFlight[position], lastSelected: s.lastSelectedAt[candidate.Credential.ID],
		}
		if candidate.Billing != nil {
			score.remaining = candidate.Billing.Remaining()
			score.billingFresh = now.Sub(candidate.Billing.SyncedAt) <= 30*time.Minute
		}
		scores = append(scores, score)
	}
	s.selectionMu.RUnlock()
	plan := &candidatePlan{values: values, scores: scores}
	heap.Init(plan)
	return plan, nil
}

// loadConcurrencySnapshotByAccountIDs merges identical account-id batches briefly.
// Snapshot is sort-only; atomic Acquire remains the capacity authority.
// Returned map is sparse (only count>0); missing ids mean 0.
func (s *Selector) loadConcurrencySnapshotByAccountIDs(ctx context.Context, accountIDs []uint64) (map[uint64]int, error) {
	cacheKey := concurrencyAccountSnapshotKey(accountIDs)
	load := func() (map[uint64]int, error) {
		if accountReader, ok := s.concurrency.(repository.AccountConcurrencySnapshotReader); ok {
			values, err := accountReader.CurrentManyAccountIDs(ctx, accountIDs)
			if err != nil {
				return nil, fmt.Errorf("批量读取账号并发租约: %w", err)
			}
			if values == nil {
				values = map[uint64]int{}
			}
			return values, nil
		}
		// Fallback: string-key bulk read, then sparsify by account id.
		keys := make([]string, len(accountIDs))
		for index, id := range accountIDs {
			keys[index] = accountConcurrencyKey(id)
		}
		dense := make(map[string]int, len(keys))
		if batchReader, ok := s.concurrency.(repository.ConcurrencySnapshotReader); ok {
			var err error
			dense, err = batchReader.CurrentMany(ctx, keys)
			if err != nil {
				return nil, fmt.Errorf("批量读取账号并发租约: %w", err)
			}
		} else {
			for _, key := range keys {
				current, err := s.concurrency.Current(ctx, key)
				if err != nil {
					return nil, fmt.Errorf("读取账号并发租约: %w", err)
				}
				if current > 0 {
					dense[key] = current
				}
			}
		}
		values := make(map[uint64]int)
		for index, key := range keys {
			if count := dense[key]; count > 0 {
				values[accountIDs[index]] = count
			}
		}
		return values, nil
	}
	// 仅测试中的手工 Selector 可能没有初始化缓存，保持最小兼容回退。
	if s.concurrencySnapshots == nil {
		return load()
	}
	return s.concurrencySnapshots.Load(ctx, cacheKey, time.Now(), load)
}

func concurrencyAccountSnapshotKey(accountIDs []uint64) [32]byte {
	hash := sha256.New()
	var buf [8]byte
	for _, id := range accountIDs {
		binary.LittleEndian.PutUint64(buf[:], id)
		_, _ = hash.Write(buf[:])
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func accountConcurrencyKey(accountID uint64) string {
	return repository.AccountConcurrencyKey(accountID)
}
