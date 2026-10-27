package main

import (
	"database/sql"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"
)

// アプリ内で配車処理を回す間隔
const matchingInterval = 100 * time.Millisecond

// 配車処理と初期化を直列化する
var matchingMu sync.Mutex

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
// アプリ内のループでも同じ処理を回しているので、呼ばれなくても配車は進む
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	runMatching()
	w.WriteHeader(http.StatusNoContent)
}

func startMatchingLoop() {
	go func() {
		for range time.Tick(matchingInterval) {
			runMatching()
		}
	}()
}

type matchedPair struct {
	ride  *rideState
	chair *chairState
}

func runMatching() {
	matchingMu.Lock()
	defer matchingMu.Unlock()

	matchedAt := now()
	for _, p := range assignChairs(matchedAt) {
		enqueueExec("UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", p.chair.ID, matchedAt, p.ride.ID)
		chairNotifier.notify(p.chair.ID)
	}
}

// これ以上待たせたライドは、遠い椅子であっても古い順に割り当てる
// ベンチマーカーはマッチングまで100tick(3秒)以内なら高く評価する
const urgentWaitDuration = 10 * time.Second

type freeChair struct {
	chair    *chairState
	location *chairLocationState
}

func (c freeChair) distanceTo(ride *rideState) int {
	return calculateDistance(c.location.latitude, c.location.longitude, ride.PickupLatitude, ride.PickupLongitude)
}

func (c freeChair) pickupTime(ride *rideState) float64 {
	return float64(c.distanceTo(ride)) / float64(max(c.chair.speed, 1))
}

// 待機中のライドに空いている椅子を割り当てる
// 組み合わせの計算は読み取りロックで行い、割り当ての反映だけ書き込みロックで行う。
// 待機ライドを減らして空き椅子を埋めるのは配車処理だけで、matchingMu で直列化されているので、
// 計算中に変わりうるのは椅子の稼働状態と新しいライドの追加だけ。
func assignChairs(matchedAt time.Time) []matchedPair {
	mu.RLock()
	rides := slices.Clone(waitingRides)
	chairs := []freeChair{}
	if len(rides) > 0 {
		for _, chair := range chairList {
			if location := chair.location.Load(); chair.IsActive && chair.free.Load() && location != nil {
				chairs = append(chairs, freeChair{chair: chair, location: location})
			}
		}
	}
	mu.RUnlock()
	if len(rides) == 0 || len(chairs) == 0 {
		return nil
	}

	rideUsed := make([]bool, len(rides))
	chairUsed := make([]bool, len(chairs))
	type plan struct{ ride, chair int }
	plans := []plan{}
	assign := func(i, j int) {
		rideUsed[i] = true
		chairUsed[j] = true
		plans = append(plans, plan{ride: i, chair: j})
	}

	// 長く待たせているライドは、古い順に最寄りの空き椅子を確保する
	for i, ride := range rides {
		if matchedAt.Sub(ride.CreatedAt) < urgentWaitDuration {
			continue
		}
		best := -1
		for j, chair := range chairs {
			if !chairUsed[j] && (best < 0 || chair.pickupTime(ride) < chairs[best].pickupTime(ride)) {
				best = j
			}
		}
		if best < 0 {
			break
		}
		assign(i, best)
	}

	// 残りは乗車位置までの所要時間が短い組から割り当てる
	// 迎車距離が速度の10倍以上だと評価が下がるので、そういう椅子しかなければ近くの椅子が空くのを待つ
	type candidate struct {
		ride  int
		chair int
		cost  float64
	}
	candidates := []candidate{}
	for i, ride := range rides {
		if rideUsed[i] {
			continue
		}
		for j, chair := range chairs {
			if chairUsed[j] || chair.distanceTo(ride) >= 10*chair.chair.speed {
				continue
			}
			candidates = append(candidates, candidate{ride: i, chair: j, cost: chair.pickupTime(ride)})
		}
	}
	sort.Slice(candidates, func(a, b int) bool { return candidates[a].cost < candidates[b].cost })
	for _, c := range candidates {
		if rideUsed[c.ride] || chairUsed[c.chair] {
			continue
		}
		assign(c.ride, c.chair)
	}
	if len(plans) == 0 {
		return nil
	}

	mu.Lock()
	defer mu.Unlock()
	pairs := make([]matchedPair, 0, len(plans))
	matched := make(map[*rideState]bool, len(plans))
	for _, p := range plans {
		ride := rides[p.ride]
		chair := chairs[p.chair].chair
		// 計算中に稼働を止めた椅子には割り当てない
		if !chair.IsActive {
			continue
		}
		ride.ChairID = sql.NullString{String: chair.ID, Valid: true}
		ride.UpdatedAt = matchedAt
		chair.free.Store(false)
		chair.latestRide = ride
		chair.activeRides++
		matched[ride] = true
		pairs = append(pairs, matchedPair{ride: ride, chair: chair})
	}

	remaining := make([]*rideState, 0, len(waitingRides))
	for _, ride := range waitingRides {
		if !matched[ride] {
			remaining = append(remaining, ride)
		}
	}
	waitingRides = remaining

	return pairs
}
