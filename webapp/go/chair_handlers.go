package main

import (
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"

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
		}
		chair.free.Store(true)
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

	mu.Lock()
	chair.IsActive = req.IsActive
	mu.Unlock()
	enqueueExec("UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)

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

	// 最新位置と累積移動距離を更新する。頻繁に呼ばれるので全体のロックは取らない
	chair.moveTo(req.Latitude, req.Longitude, recordedAt)

	// 乗車位置・目的地への到着を判定する。到着したときだけ書き込みロックを取る
	arrivedStatus := func(ride *rideState) string {
		if ride == nil {
			return ""
		}
		switch ride.latestStatus() {
		case "ENROUTE":
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude {
				return "PICKUP"
			}
		case "CARRYING":
			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude {
				return "ARRIVED"
			}
		}
		return ""
	}
	mu.RLock()
	arrived := arrivedStatus(chair.latestRide) != ""
	mu.RUnlock()
	if arrived {
		ride, status := func() (*rideState, *rideStatusState) {
			mu.Lock()
			defer mu.Unlock()
			ride := chair.latestRide
			if next := arrivedStatus(ride); next != "" {
				return ride, appendRideStatusLocked(ride, next)
			}
			return nil, nil
		}()
		if status != nil {
			notifyRide(ride.UserID, ride.ChairID.String)
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

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	chair := r.Context().Value("chair").(*chairState)

	streamNotifications(w, r, chairNotifier.channel(chair.ID), "chair_sent_at", func(resendLatest bool) (any, *rideStatusState) {
		data, sent := nextChairNotification(chair, resendLatest)
		if data == nil {
			return nil, nil
		}
		return data, sent
	})
}

// 割り当てられた最新のライドについて、未通知の状態があれば古い順に1つ返して通知済みにする
// 未通知が無ければ、resendLatest のときだけ最新の状態を返す
func nextChairNotification(chair *chairState, resendLatest bool) (*chairGetNotificationResponseData, *rideStatusState) {
	mu.RLock()
	defer mu.RUnlock()

	ride := chair.latestRide
	if ride == nil {
		return nil, nil
	}

	status := ride.latestStatus()
	sent := ride.claimUnsentStatus(func(s *rideStatusState) *atomic.Bool { return &s.chairSent })
	if sent != nil {
		status = sent.status
		// 完了を椅子に伝えるまでは次のライドを割り当てない
		if sent.status == "COMPLETED" {
			chair.free.Store(true)
		}
	} else if !resendLatest {
		return nil, nil
	}

	data := &chairGetNotificationResponseData{
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
	}
	if user := usersByID[ride.UserID]; user != nil {
		data.User = simpleUser{
			ID:   user.ID,
			Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
		}
	}

	return data, sent
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
		addRideStatus(ride, "ENROUTE")
	// After Picking up user
	case "CARRYING":
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		addRideStatus(ride, "CARRYING")
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
