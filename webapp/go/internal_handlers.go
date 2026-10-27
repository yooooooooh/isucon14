package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
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
	runMatching(r.Context())
	w.WriteHeader(http.StatusNoContent)
}

func startMatchingLoop() {
	go func() {
		for range time.Tick(matchingInterval) {
			runMatching(context.Background())
		}
	}()
}

type matchedPair struct {
	ride  *rideState
	chair *chairState
}

func runMatching(ctx context.Context) {
	matchingMu.Lock()
	defer matchingMu.Unlock()

	matchedAt := now()
	for _, p := range assignChairs(matchedAt) {
		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", p.chair.ID, matchedAt, p.ride.ID); err != nil {
			slog.Error("failed to update matched ride", "error", err)
		}
	}
}

// これ以上待たせたライドは、遠い椅子であっても古い順に割り当てる
// ベンチマーカーはマッチングまで100tick(3秒)以内なら高く評価する
const urgentWaitDuration = 10 * time.Second

func pickupTime(ride *rideState, chair *chairState) float64 {
	distance := calculateDistance(chair.latitude, chair.longitude, ride.PickupLatitude, ride.PickupLongitude)
	return float64(distance) / float64(max(chair.speed, 1))
}

// 待機中のライドに空いている椅子を割り当てる
func assignChairs(matchedAt time.Time) []matchedPair {
	mu.Lock()
	defer mu.Unlock()

	if len(waitingRides) == 0 {
		return nil
	}
	chairs := []*chairState{}
	for _, chair := range chairList {
		if chair.IsActive && chair.free && chair.hasLocation {
			chairs = append(chairs, chair)
		}
	}
	if len(chairs) == 0 {
		return nil
	}

	rideUsed := make([]bool, len(waitingRides))
	chairUsed := make([]bool, len(chairs))
	pairs := []matchedPair{}
	assign := func(i, j int) {
		rideUsed[i] = true
		chairUsed[j] = true
		ride := waitingRides[i]
		chair := chairs[j]
		ride.ChairID = sql.NullString{String: chair.ID, Valid: true}
		ride.UpdatedAt = matchedAt
		chair.free = false
		chair.latestRide = ride
		chair.activeRides++
		pairs = append(pairs, matchedPair{ride: ride, chair: chair})
	}

	// 長く待たせているライドは、古い順に最寄りの空き椅子を確保する
	for i, ride := range waitingRides {
		if matchedAt.Sub(ride.CreatedAt) < urgentWaitDuration {
			continue
		}
		best := -1
		for j, chair := range chairs {
			if !chairUsed[j] && (best < 0 || pickupTime(ride, chair) < pickupTime(ride, chairs[best])) {
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
	for i, ride := range waitingRides {
		if rideUsed[i] {
			continue
		}
		for j, chair := range chairs {
			if chairUsed[j] {
				continue
			}
			distance := calculateDistance(chair.latitude, chair.longitude, ride.PickupLatitude, ride.PickupLongitude)
			if distance >= 10*chair.speed {
				continue
			}
			candidates = append(candidates, candidate{ride: i, chair: j, cost: pickupTime(ride, chair)})
		}
	}
	sort.Slice(candidates, func(a, b int) bool { return candidates[a].cost < candidates[b].cost })
	for _, c := range candidates {
		if rideUsed[c.ride] || chairUsed[c.chair] {
			continue
		}
		assign(c.ride, c.chair)
	}

	remaining := make([]*rideState, 0, len(waitingRides)-len(pairs))
	for i, ride := range waitingRides {
		if !rideUsed[i] {
			remaining = append(remaining, ride)
		}
	}
	waitingRides = remaining

	return pairs
}
