package main

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// アプリケーションの状態をメモリに保持する。
// 書き込みは従来どおりDBにも行い、DBを正として起動時と初期化時にメモリへ再構築する。
var (
	mu sync.RWMutex

	usersByID                  map[string]*User
	usersByToken               map[string]*User
	ownersByID                 map[string]*Owner
	ownersByToken              map[string]*Owner
	ownersByChairRegisterToken map[string]*Owner
	chairList                  []*chairState
	chairsByID                 map[string]*chairState
	chairsByToken              map[string]*chairState
	chairsByOwner              map[string][]*chairState
	chairModelSpeeds           map[string]int
	paymentTokens              map[string]string
	ridesByID                  map[string]*rideState
	ridesByUser                map[string][]*rideState // created_at 昇順
	waitingRides               []*rideState            // 椅子未割り当てのライド (created_at 昇順)
	paymentGatewayURL          string
)

type chairState struct {
	Chair
	speed int

	hasLocation       bool
	latitude          int
	longitude         int
	totalDistance     int
	locationUpdatedAt time.Time

	// 最後に割り当てられたライド
	latestRide *rideState
	// 割り当て済みで COMPLETED になっていないライドの数
	activeRides int
	// 割り当てたすべてのライドの完了を椅子へ通知済みで、新しいライドを割り当てられる
	free bool

	// 売上計算用の完了済みライド
	completedRides []*rideState
	// 通知で返す統計情報
	totalRideCount  int
	totalEvaluation int
}

type rideState struct {
	Ride
	// 適用されたクーポンの割引額
	discount int
	statuses []*rideStatusState
}

type rideStatusState struct {
	id        string
	status    string
	createdAt time.Time
	appSent   bool
	chairSent bool
}

func (r *rideState) latestStatus() string {
	return r.statuses[len(r.statuses)-1].status
}

func (r *rideState) hasStatus(status string) bool {
	for _, s := range r.statuses {
		if s.status == status {
			return true
		}
	}
	return false
}

func (r *rideState) fare() int {
	meteredFare := farePerDistance * calculateDistance(r.PickupLatitude, r.PickupLongitude, r.DestinationLatitude, r.DestinationLongitude)
	return initialFare + max(meteredFare-r.discount, 0)
}

func (c *chairState) recordCompletedRide(r *rideState) {
	c.activeRides--
	c.completedRides = append(c.completedRides, r)
	if r.hasStatus("ARRIVED") && r.hasStatus("CARRYING") && r.Evaluation != nil {
		c.totalRideCount++
		c.totalEvaluation += *r.Evaluation
	}
}

// DATETIME(6) に合わせてマイクロ秒に丸めた現在時刻
func now() time.Time {
	return time.Now().Truncate(time.Microsecond)
}

// ライドの状態遷移をDBに記録してからメモリに反映する
func addRideStatus(ctx context.Context, ride *rideState, status string) error {
	s := &rideStatusState{id: ulid.Make().String(), status: status, createdAt: now()}
	if _, err := db.ExecContext(
		ctx,
		"INSERT INTO ride_statuses (id, ride_id, status, created_at) VALUES (?, ?, ?, ?)",
		s.id, ride.ID, s.status, s.createdAt,
	); err != nil {
		return err
	}
	mu.Lock()
	ride.statuses = append(ride.statuses, s)
	mu.Unlock()
	return nil
}

func loadState(ctx context.Context) error {
	users := []*User{}
	if err := db.SelectContext(ctx, &users, "SELECT * FROM users"); err != nil {
		return err
	}
	owners := []*Owner{}
	if err := db.SelectContext(ctx, &owners, "SELECT * FROM owners"); err != nil {
		return err
	}
	chairs := []Chair{}
	if err := db.SelectContext(ctx, &chairs, "SELECT * FROM chairs ORDER BY id"); err != nil {
		return err
	}
	models := []ChairModel{}
	if err := db.SelectContext(ctx, &models, "SELECT * FROM chair_models"); err != nil {
		return err
	}
	tokens := []PaymentToken{}
	if err := db.SelectContext(ctx, &tokens, "SELECT * FROM payment_tokens"); err != nil {
		return err
	}
	locations := []ChairLocation{}
	if err := db.SelectContext(ctx, &locations, "SELECT * FROM chair_locations ORDER BY chair_id, created_at"); err != nil {
		return err
	}
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, "SELECT * FROM rides ORDER BY created_at"); err != nil {
		return err
	}
	statuses := []RideStatus{}
	if err := db.SelectContext(ctx, &statuses, "SELECT * FROM ride_statuses ORDER BY ride_id, created_at, status"); err != nil {
		return err
	}
	usedCoupons := []Coupon{}
	if err := db.SelectContext(ctx, &usedCoupons, "SELECT * FROM coupons WHERE used_by IS NOT NULL"); err != nil {
		return err
	}
	var gatewayURL string
	if err := db.GetContext(ctx, &gatewayURL, "SELECT value FROM settings WHERE name = 'payment_gateway_url'"); err != nil {
		return err
	}

	newUsersByID := make(map[string]*User, len(users))
	newUsersByToken := make(map[string]*User, len(users))
	for _, u := range users {
		newUsersByID[u.ID] = u
		newUsersByToken[u.AccessToken] = u
	}

	newOwnersByID := make(map[string]*Owner, len(owners))
	newOwnersByToken := make(map[string]*Owner, len(owners))
	newOwnersByChairRegisterToken := make(map[string]*Owner, len(owners))
	for _, o := range owners {
		newOwnersByID[o.ID] = o
		newOwnersByToken[o.AccessToken] = o
		newOwnersByChairRegisterToken[o.ChairRegisterToken] = o
	}

	newChairModelSpeeds := make(map[string]int, len(models))
	for _, m := range models {
		newChairModelSpeeds[m.Name] = m.Speed
	}

	newChairList := make([]*chairState, 0, len(chairs))
	newChairsByID := make(map[string]*chairState, len(chairs))
	newChairsByToken := make(map[string]*chairState, len(chairs))
	newChairsByOwner := map[string][]*chairState{}
	for _, c := range chairs {
		cs := &chairState{Chair: c, speed: newChairModelSpeeds[c.Model], free: true}
		newChairList = append(newChairList, cs)
		newChairsByID[c.ID] = cs
		newChairsByToken[c.AccessToken] = cs
		newChairsByOwner[c.OwnerID] = append(newChairsByOwner[c.OwnerID], cs)
	}

	for _, l := range locations {
		cs := newChairsByID[l.ChairID]
		if cs == nil {
			continue
		}
		if cs.hasLocation {
			cs.totalDistance += calculateDistance(cs.latitude, cs.longitude, l.Latitude, l.Longitude)
		}
		cs.hasLocation = true
		cs.latitude = l.Latitude
		cs.longitude = l.Longitude
		cs.locationUpdatedAt = l.CreatedAt
	}

	newPaymentTokens := make(map[string]string, len(tokens))
	for _, t := range tokens {
		newPaymentTokens[t.UserID] = t.Token
	}

	discounts := make(map[string]int, len(usedCoupons))
	for _, c := range usedCoupons {
		discounts[*c.UsedBy] = c.Discount
	}

	newRidesByID := make(map[string]*rideState, len(rides))
	for _, r := range rides {
		newRidesByID[r.ID] = &rideState{Ride: r, discount: discounts[r.ID]}
	}
	for _, s := range statuses {
		rs := newRidesByID[s.RideID]
		if rs == nil {
			continue
		}
		rs.statuses = append(rs.statuses, &rideStatusState{
			id:        s.ID,
			status:    s.Status,
			createdAt: s.CreatedAt,
			appSent:   s.AppSentAt != nil,
			chairSent: s.ChairSentAt != nil,
		})
	}

	newRidesByUser := map[string][]*rideState{}
	newWaitingRides := []*rideState{}
	for _, r := range rides {
		rs := newRidesByID[r.ID]
		if len(rs.statuses) == 0 {
			continue
		}
		newRidesByUser[r.UserID] = append(newRidesByUser[r.UserID], rs)
		if !r.ChairID.Valid {
			newWaitingRides = append(newWaitingRides, rs)
			continue
		}
		cs := newChairsByID[r.ChairID.String]
		if cs == nil {
			continue
		}
		if cs.latestRide == nil || rs.UpdatedAt.After(cs.latestRide.UpdatedAt) {
			cs.latestRide = rs
		}
		cs.activeRides++
		if rs.latestStatus() == "COMPLETED" {
			cs.recordCompletedRide(rs)
		}
		// 6つの状態すべてを椅子へ通知し終えるまでは次のライドを割り当てない
		allSent := len(rs.statuses) == 6
		for _, s := range rs.statuses {
			allSent = allSent && s.chairSent
		}
		if !allSent {
			cs.free = false
		}
	}
	for _, cs := range newChairList {
		sort.SliceStable(cs.completedRides, func(i, j int) bool {
			return cs.completedRides[i].UpdatedAt.Before(cs.completedRides[j].UpdatedAt)
		})
	}

	mu.Lock()
	defer mu.Unlock()
	usersByID = newUsersByID
	usersByToken = newUsersByToken
	ownersByID = newOwnersByID
	ownersByToken = newOwnersByToken
	ownersByChairRegisterToken = newOwnersByChairRegisterToken
	chairList = newChairList
	chairsByID = newChairsByID
	chairsByToken = newChairsByToken
	chairsByOwner = newChairsByOwner
	chairModelSpeeds = newChairModelSpeeds
	paymentTokens = newPaymentTokens
	ridesByID = newRidesByID
	ridesByUser = newRidesByUser
	waitingRides = newWaitingRides
	paymentGatewayURL = gatewayURL
	return nil
}
