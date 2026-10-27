package main

import (
	"context"
	"errors"
	"net/http"
)

func appAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("app_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("app_session cookie is required"))
			return
		}
		mu.RLock()
		user := usersByToken[c.Value]
		mu.RUnlock()
		if user == nil {
			writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
			return
		}

		ctx = context.WithValue(ctx, "user", user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func ownerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("owner_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("owner_session cookie is required"))
			return
		}
		mu.RLock()
		owner := ownersByToken[c.Value]
		mu.RUnlock()
		if owner == nil {
			writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
			return
		}

		ctx = context.WithValue(ctx, "owner", owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func chairAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("chair_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("chair_session cookie is required"))
			return
		}
		mu.RLock()
		chair := chairsByToken[c.Value]
		mu.RUnlock()
		if chair == nil {
			writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
			return
		}

		ctx = context.WithValue(ctx, "chair", chair)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
