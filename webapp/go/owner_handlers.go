package main

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	initialFare     = 500
	farePerDistance = 100
)

type ownerPostOwnersRequest struct {
	Name string `json:"name"`
}

type ownerPostOwnersResponse struct {
	ID                 string `json:"id"`
	ChairRegisterToken string `json:"chair_register_token"`
}

func ownerPostOwners(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &ownerPostOwnersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name) are empty"))
		return
	}

	ownerID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	chairRegisterToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO owners (id, name, access_token, chair_register_token) VALUES (?, ?, ?, ?)",
		ownerID, req.Name, accessToken, chairRegisterToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	owner := &Owner{
		ID:                 ownerID,
		Name:               req.Name,
		AccessToken:        accessToken,
		ChairRegisterToken: chairRegisterToken,
	}
	mu.Lock()
	ownersByID[ownerID] = owner
	ownersByToken[accessToken] = owner
	ownersByChairRegisterToken[chairRegisterToken] = owner
	mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "owner_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &ownerPostOwnersResponse{
		ID:                 ownerID,
		ChairRegisterToken: chairRegisterToken,
	})
}

type chairSales struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Sales int    `json:"sales"`
}

type modelSales struct {
	Model string `json:"model"`
	Sales int    `json:"sales"`
}

type ownerGetSalesResponse struct {
	TotalSales int          `json:"total_sales"`
	Chairs     []chairSales `json:"chairs"`
	Models     []modelSales `json:"models"`
}

func ownerGetSales(w http.ResponseWriter, r *http.Request) {
	since := time.Unix(0, 0)
	until := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if r.URL.Query().Get("since") != "" {
		parsed, err := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		since = time.UnixMilli(parsed)
	}
	if r.URL.Query().Get("until") != "" {
		parsed, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		until = time.UnixMilli(parsed)
	}
	// ミリ秒で指定された until の範囲内に完了したライドを含める
	until = until.Add(999 * time.Microsecond)

	owner := r.Context().Value("owner").(*Owner)

	res := ownerGetSalesResponse{
		TotalSales: 0,
	}

	modelSalesByModel := map[string]int{}
	func() {
		mu.RLock()
		defer mu.RUnlock()
		for _, chair := range chairsByOwner[owner.ID] {
			sales := 0
			for _, ride := range chair.completedRides {
				if ride.UpdatedAt.Before(since) || ride.UpdatedAt.After(until) {
					continue
				}
				sales += calculateSale(ride.Ride)
			}
			res.TotalSales += sales

			res.Chairs = append(res.Chairs, chairSales{
				ID:    chair.ID,
				Name:  chair.Name,
				Sales: sales,
			})

			modelSalesByModel[chair.Model] += sales
		}
	}()

	models := []modelSales{}
	for model, sales := range modelSalesByModel {
		models = append(models, modelSales{
			Model: model,
			Sales: sales,
		})
	}
	res.Models = models

	writeJSON(w, http.StatusOK, res)
}

func calculateSale(ride Ride) int {
	return calculateFare(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
}

type ownerGetChairResponse struct {
	Chairs []ownerGetChairResponseChair `json:"chairs"`
}

type ownerGetChairResponseChair struct {
	ID                     string `json:"id"`
	Name                   string `json:"name"`
	Model                  string `json:"model"`
	Active                 bool   `json:"active"`
	RegisteredAt           int64  `json:"registered_at"`
	TotalDistance          int    `json:"total_distance"`
	TotalDistanceUpdatedAt *int64 `json:"total_distance_updated_at,omitempty"`
}

func ownerGetChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	owner := ctx.Value("owner").(*Owner)

	res := ownerGetChairResponse{}
	func() {
		mu.RLock()
		defer mu.RUnlock()
		for _, chair := range chairsByOwner[owner.ID] {
			c := ownerGetChairResponseChair{
				ID:           chair.ID,
				Name:         chair.Name,
				Model:        chair.Model,
				Active:       chair.IsActive,
				RegisteredAt: chair.CreatedAt.UnixMilli(),
			}
			if location := chair.location.Load(); location != nil {
				c.TotalDistance = location.totalDistance
				t := location.updatedAt.UnixMilli()
				c.TotalDistanceUpdatedAt = &t
			}
			res.Chairs = append(res.Chairs, c)
		}
	}()
	writeJSON(w, http.StatusOK, res)
}
