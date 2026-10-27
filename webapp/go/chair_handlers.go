package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	mu.RLock()
	owner := ownersByChairRegisterToken[req.ChairRegisterToken]
	mu.RUnlock()
	if owner == nil {
		writeError(w, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	createdAt := now()

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken, createdAt, createdAt,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	func() {
		mu.Lock()
		defer mu.Unlock()
		chair := &chairState{
			Chair: Chair{
				ID:          chairID,
				OwnerID:     owner.ID,
				Name:        req.Name,
				Model:       req.Model,
				IsActive:    false,
				AccessToken: accessToken,
				CreatedAt:   createdAt,
				UpdatedAt:   createdAt,
			},
			speed: chairModelSpeeds[req.Model],
			free:  true,
		}
		chairList = append(chairList, chair)
		chairsByID[chairID] = chair
		chairsByToken[accessToken] = chair
		chairsByOwner[owner.ID] = append(chairsByOwner[owner.ID], chair)
	}()

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*chairState)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := db.ExecContext(ctx, "UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	mu.Lock()
	chair.IsActive = req.IsActive
	mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*chairState)
	recordedAt := now()

	// 椅子はこのAPIが応答するまで次の移動に進めないので、DBへの記録はまとめて非同期に行う
	enqueueChairLocation(ChairLocation{
		ID:        ulid.Make().String(),
		ChairID:   chair.ID,
		Latitude:  req.Latitude,
		Longitude: req.Longitude,
		CreatedAt: recordedAt,
	})

	// 最新位置と累積移動距離を更新し、乗車位置・目的地への到着を判定する
	ride, nextStatus := func() (*rideState, string) {
		mu.Lock()
		defer mu.Unlock()

		if chair.hasLocation {
			chair.totalDistance += calculateDistance(chair.latitude, chair.longitude, req.Latitude, req.Longitude)
		}
		chair.hasLocation = true
		chair.latitude = req.Latitude
		chair.longitude = req.Longitude
		chair.locationUpdatedAt = recordedAt

		ride := chair.latestRide
		if ride == nil {
			return nil, ""
		}
		switch ride.latestStatus() {
		case "ENROUTE":
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude {
				return ride, "PICKUP"
			}
		case "CARRYING":
			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude {
				return ride, "ARRIVED"
			}
		}
		return nil, ""
	}()
	if ride != nil {
		if err := addRideStatus(ctx, ride, nextStatus); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: recordedAt.UnixMilli(),
	})
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponse struct {
	Data         *chairGetNotificationResponseData `json:"data"`
	RetryAfterMs int                               `json:"retry_after_ms"`
}

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*chairState)

	response, sent := buildChairNotification(chair)
	if sent != nil {
		// メモリ上は送信済みにしているので、DB更新に失敗しても通知は返す
		if _, err := db.ExecContext(context.WithoutCancel(ctx), `UPDATE ride_statuses SET chair_sent_at = ? WHERE id = ?`, now(), sent.id); err != nil {
			slog.Error("failed to update chair_sent_at", "error", err)
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// 割り当てられた最新のライドについて、未通知の状態があれば古い順に1つ、無ければ最新の状態を返す
func buildChairNotification(chair *chairState) (*chairGetNotificationResponse, *rideStatusState) {
	mu.Lock()
	defer mu.Unlock()

	ride := chair.latestRide
	if ride == nil {
		return &chairGetNotificationResponse{RetryAfterMs: notificationRetryAfterMs}, nil
	}

	status := ride.latestStatus()
	var sent *rideStatusState
	for _, s := range ride.statuses {
		if !s.chairSent {
			s.chairSent = true
			sent = s
			status = s.status
			// 完了を椅子に伝えるまでは次のライドを割り当てない
			if s.status == "COMPLETED" {
				chair.free = true
			}
			break
		}
	}

	response := &chairGetNotificationResponse{
		Data: &chairGetNotificationResponseData{
			RideID: ride.ID,
			PickupCoordinate: Coordinate{
				Latitude:  ride.PickupLatitude,
				Longitude: ride.PickupLongitude,
			},
			DestinationCoordinate: Coordinate{
				Latitude:  ride.DestinationLatitude,
				Longitude: ride.DestinationLongitude,
			},
			Status: status,
		},
		RetryAfterMs: notificationRetryAfterMs,
	}
	if user := usersByID[ride.UserID]; user != nil {
		response.Data.User = simpleUser{
			ID:   user.ID,
			Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
		}
	}

	return response, sent
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*chairState)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var (
		ride    *rideState
		status  string
		chairID string
	)
	func() {
		mu.RLock()
		defer mu.RUnlock()
		ride = ridesByID[rideID]
		if ride != nil {
			status = ride.latestStatus()
			chairID = ride.ChairID.String
		}
	}()
	if ride == nil {
		writeError(w, http.StatusNotFound, errors.New("ride not found"))
		return
	}
	if chairID != chair.ID {
		writeError(w, http.StatusBadRequest, errors.New("not assigned to this ride"))
		return
	}

	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		if err := addRideStatus(ctx, ride, "ENROUTE"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	// After Picking up user
	case "CARRYING":
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		if err := addRideStatus(ctx, ride, "CARRYING"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
