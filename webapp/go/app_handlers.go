package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/oklog/ulid/v2"
)

// 通知エンドポイントのポーリング間隔
const notificationRetryAfterMs = 30

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	userID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	invitationCode := secureRandomStr(15)

	// 招待数の上限チェックを招待コード単位で直列化する
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		defer lockInvitationCode(*req.InvitationCode)()
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userID, req.Username, req.FirstName, req.LastName, req.DateOfBirth, accessToken, invitationCode,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 初回登録キャンペーンのクーポンを付与
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
		userID, "CP_NEW2024", 3000,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 招待コードを使った登録
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		// 招待する側の招待数をチェック
		var coupons []Coupon
		err = tx.SelectContext(ctx, &coupons, "SELECT * FROM coupons WHERE code = ?", "INV_"+*req.InvitationCode)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if len(coupons) >= 3 {
			writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
			return
		}

		// ユーザーチェック
		var inviter User
		err = tx.GetContext(ctx, &inviter, "SELECT * FROM users WHERE invitation_code = ?", *req.InvitationCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// 招待クーポン付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
			userID, "INV_"+*req.InvitationCode, 1500,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		// 招待した人にもRewardを付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, CONCAT(?, '_', FLOOR(UNIX_TIMESTAMP(NOW(3))*1000)), ?)",
			inviter.ID, "RWD_"+*req.InvitationCode, 1000,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	user := &User{
		ID:             userID,
		Username:       req.Username,
		Firstname:      req.FirstName,
		Lastname:       req.LastName,
		DateOfBirth:    req.DateOfBirth,
		AccessToken:    accessToken,
		InvitationCode: invitationCode,
	}
	mu.Lock()
	usersByID[userID] = user
	usersByToken[accessToken] = user
	mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

var invitationCodeLocks sync.Map

func lockInvitationCode(code string) (unlock func()) {
	l, _ := invitationCodeLocks.LoadOrStore(code, &sync.Mutex{})
	m := l.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user := ctx.Value("user").(*User)

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)`,
		user.ID,
		req.Token,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	mu.Lock()
	paymentTokens[user.ID] = req.Token
	mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	items := func() []getAppRidesResponseItem {
		mu.RLock()
		defer mu.RUnlock()

		items := []getAppRidesResponseItem{}
		rides := ridesByUser[user.ID]
		// created_at の降順で返す
		for i := len(rides) - 1; i >= 0; i-- {
			ride := rides[i]
			if ride.latestStatus() != "COMPLETED" {
				continue
			}
			chair := chairsByID[ride.ChairID.String]
			item := getAppRidesResponseItem{
				ID:                    ride.ID,
				PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
				DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
				Fare:                  ride.fare(),
				RequestedAt:           ride.CreatedAt.UnixMilli(),
				CompletedAt:           ride.UpdatedAt.UnixMilli(),
			}
			if ride.Evaluation != nil {
				item.Evaluation = *ride.Evaluation
			}
			if chair != nil {
				item.Chair = getAppRidesResponseItemChair{
					ID:    chair.ID,
					Name:  chair.Name,
					Model: chair.Model,
				}
				if owner := ownersByID[chair.OwnerID]; owner != nil {
					item.Chair.Owner = owner.Name
				}
			}
			items = append(items, item)
		}
		return items
	}()

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	continuing, rideCount := func() (bool, int) {
		mu.RLock()
		defer mu.RUnlock()
		rides := ridesByUser[user.ID]
		for _, ride := range rides {
			if ride.latestStatus() != "COMPLETED" {
				return true, 0
			}
		}
		return false, len(rides) + 1
	}()
	if continuing {
		writeError(w, http.StatusConflict, errors.New("ride already exists"))
		return
	}

	rideID := ulid.Make().String()
	status := &rideStatusState{id: ulid.Make().String(), status: "MATCHING", createdAt: now()}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude, created_at, updated_at)
				  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rideID, user.ID, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude, status.createdAt, status.createdAt,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO ride_statuses (id, ride_id, status, created_at) VALUES (?, ?, ?, ?)`,
		status.id, rideID, status.status, status.createdAt,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var coupon Coupon
	couponUsed := false
	if rideCount == 1 {
		// 初回利用で、初回利用クーポンがあれば必ず使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			couponUsed = true
		}
	}
	if !couponUsed {
		// 他のクーポンを付与された順番に使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			couponUsed = true
		}
	}
	discount := 0
	if couponUsed {
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
			rideID, user.ID, coupon.Code,
		); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		discount = coupon.Discount
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	ride := &rideState{
		Ride: Ride{
			ID:                   rideID,
			UserID:               user.ID,
			PickupLatitude:       req.PickupCoordinate.Latitude,
			PickupLongitude:      req.PickupCoordinate.Longitude,
			DestinationLatitude:  req.DestinationCoordinate.Latitude,
			DestinationLongitude: req.DestinationCoordinate.Longitude,
			CreatedAt:            status.createdAt,
			UpdatedAt:            status.createdAt,
		},
		discount: discount,
		statuses: []*rideStatusState{status},
	}
	mu.Lock()
	ridesByID[rideID] = ride
	ridesByUser[user.ID] = append(ridesByUser[user.ID], ride)
	waitingRides = append(waitingRides, ride)
	mu.Unlock()

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   ride.fare(),
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	discount, err := estimateDiscount(ctx, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare := calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	discounted := initialFare + max(fare-initialFare-discount, 0)

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: fare - discounted,
	})
}

// 次のライドで適用されるクーポンの割引額を求める
func estimateDiscount(ctx context.Context, userID string) (int, error) {
	var coupon Coupon
	// 初回利用クーポンを最優先で使う
	if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err == nil {
		return coupon.Discount, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	// 無いなら他のクーポンを付与された順番に使う
	if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err == nil {
		return coupon.Discount, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return 0, nil
}

// マンハッタン距離を求める
func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	var (
		ride       *rideState
		status     string
		token      string
		hasToken   bool
		gatewayURL string
	)
	func() {
		mu.RLock()
		defer mu.RUnlock()
		ride = ridesByID[rideID]
		if ride != nil {
			status = ride.latestStatus()
			token, hasToken = paymentTokens[ride.UserID]
		}
		gatewayURL = paymentGatewayURL
	}()
	if ride == nil {
		writeError(w, http.StatusNotFound, errors.New("ride not found"))
		return
	}
	if status != "ARRIVED" {
		writeError(w, http.StatusBadRequest, errors.New("not arrived yet"))
		return
	}
	if !hasToken {
		writeError(w, http.StatusBadRequest, errors.New("payment token not registered"))
		return
	}

	completed := &rideStatusState{id: ulid.Make().String(), status: "COMPLETED", createdAt: now()}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE rides SET evaluation = ?, updated_at = ? WHERE id = ?`,
		req.Evaluation, completed.createdAt, rideID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO ride_statuses (id, ride_id, status, created_at) VALUES (?, ?, ?, ?)`,
		completed.id, rideID, completed.status, completed.createdAt); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ベンチマーカーは評価APIの成功直後に支払い済みかを確認するので、決済が完了してから応答する
	paymentGatewayRequest := &paymentGatewayPostPaymentRequest{
		Amount: ride.fare(),
	}
	if err := requestPaymentGatewayPostPayment(ctx, gatewayURL, token, ride.ID, paymentGatewayRequest); err != nil {
		if errors.Is(err, erroredUpstream) {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	func() {
		mu.Lock()
		defer mu.Unlock()
		evaluation := req.Evaluation
		ride.Evaluation = &evaluation
		ride.UpdatedAt = completed.createdAt
		ride.statuses = append(ride.statuses, completed)
		if chair := chairsByID[ride.ChairID.String]; chair != nil {
			chair.recordCompletedRide(ride)
		}
	}()

	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{
		CompletedAt: completed.createdAt.UnixMilli(),
	})
}

type appGetNotificationResponse struct {
	Data         *appGetNotificationResponseData `json:"data"`
	RetryAfterMs int                             `json:"retry_after_ms"`
}

type appGetNotificationResponseData struct {
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

func appGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	response, sent := buildAppNotification(user.ID)
	if sent != nil {
		// メモリ上は送信済みにしているので、DB更新に失敗しても通知は返す
		if _, err := db.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = ? WHERE id = ?`, now(), sent.id); err != nil {
			slog.Error("failed to update app_sent_at", "error", err)
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// 最新のライドについて、未通知の状態があれば古い順に1つ、無ければ最新の状態を返す
func buildAppNotification(userID string) (*appGetNotificationResponse, *rideStatusState) {
	mu.Lock()
	defer mu.Unlock()

	rides := ridesByUser[userID]
	if len(rides) == 0 {
		return &appGetNotificationResponse{RetryAfterMs: notificationRetryAfterMs}, nil
	}
	ride := rides[len(rides)-1]

	status := ride.latestStatus()
	var sent *rideStatusState
	for _, s := range ride.statuses {
		if !s.appSent {
			s.appSent = true
			sent = s
			status = s.status
			break
		}
	}

	response := &appGetNotificationResponse{
		Data: &appGetNotificationResponseData{
			RideID: ride.ID,
			PickupCoordinate: Coordinate{
				Latitude:  ride.PickupLatitude,
				Longitude: ride.PickupLongitude,
			},
			DestinationCoordinate: Coordinate{
				Latitude:  ride.DestinationLatitude,
				Longitude: ride.DestinationLongitude,
			},
			Fare:      ride.fare(),
			Status:    status,
			CreatedAt: ride.CreatedAt.UnixMilli(),
			UpdateAt:  ride.UpdatedAt.UnixMilli(),
		},
		RetryAfterMs: notificationRetryAfterMs,
	}

	if chair := chairsByID[ride.ChairID.String]; ride.ChairID.Valid && chair != nil {
		stats := appGetNotificationResponseChairStats{TotalRidesCount: chair.totalRideCount}
		if chair.totalRideCount > 0 {
			stats.TotalEvaluationAvg = float64(chair.totalEvaluation) / float64(chair.totalRideCount)
		}
		response.Data.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: stats,
		}
	}

	return response, sent
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	retrievedAt := now()
	nearbyChairs := func() []appGetNearbyChairsResponseChair {
		mu.RLock()
		defer mu.RUnlock()

		nearbyChairs := []appGetNearbyChairsResponseChair{}
		for _, chair := range chairList {
			// 稼働中で、前のライドの完了を椅子に通知し終えた椅子だけを返す
			// COMPLETED になった直後に返すと、ベンチマーカー側で評価の完了処理が終わる前に検証されることがある
			if !chair.IsActive || !chair.free || chair.activeRides > 0 || !chair.hasLocation {
				continue
			}
			if calculateDistance(lat, lon, chair.latitude, chair.longitude) <= distance {
				nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
					ID:    chair.ID,
					Name:  chair.Name,
					Model: chair.Model,
					CurrentCoordinate: Coordinate{
						Latitude:  chair.latitude,
						Longitude: chair.longitude,
					},
				})
			}
		}
		return nearbyChairs
	}()

	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
		Chairs:      nearbyChairs,
		RetrievedAt: retrievedAt.UnixMilli(),
	})
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}
