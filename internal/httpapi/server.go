package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"forgequeue/internal/auth"
)

type Server struct {
	srv  *http.Server
	auth *auth.Service
	oidc *auth.OIDC
	h    *Handler

	// Login brute-force guards: one global backstop and one per client IP.
	loginGlobal *rateLimiter
	loginIP     *rateLimiter
}

func NewServer(addr string, rdb *redis.Client, authSvc *auth.Service) *Server {
	if addr == "" {
		addr = ":8080"
	}

	s := &Server{
		auth: authSvc,
		// Burst generous enough to absorb a human retyping a password; the
		// sustained rate is what stops automated guessing.
		loginGlobal: newRateLimiter(20, 40),
		loginIP:     newRateLimiter(0.2, 10),
	}
	h := NewHandler(rdb, authSvc)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			writeError(w, http.StatusServiceUnavailable, "redis unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	mux.HandleFunc("POST /api/auth/login", s.loginLimiter(h.Login))
	mux.HandleFunc("GET /api/auth/config", h.AuthConfig)
	mux.HandleFunc("GET /api/auth/oidc/login", h.OIDCLogin)
	mux.HandleFunc("GET /api/auth/oidc/callback", h.OIDCCallback)
	mux.HandleFunc("POST /api/auth/oidc/exchange", h.OIDCExchange)

	mux.HandleFunc("GET /api/auth/me", s.require("", h.Me))
	mux.HandleFunc("GET /api/system/version", s.require(auth.PermSystemRead, h.Version))
	mux.HandleFunc("GET /api/system/stats", s.require(auth.PermSystemRead, h.Stats))
	mux.HandleFunc("GET /api/queues", s.require(auth.PermQueuesRead, h.Queues))
	mux.HandleFunc("GET /api/queues/{name}", s.require(auth.PermQueuesRead, func(w http.ResponseWriter, r *http.Request) {
		h.Queue(w, r, r.PathValue("name"))
	}))
	mux.HandleFunc("GET /api/queues/{name}/jobs", s.require(auth.PermJobsRead, func(w http.ResponseWriter, r *http.Request) {
		h.QueueJobs(w, r, r.PathValue("name"))
	}))
	mux.HandleFunc("POST /api/queues/{name}/pause", s.require(auth.PermQueuesWrite, func(w http.ResponseWriter, r *http.Request) {
		h.PauseQueue(w, r, r.PathValue("name"))
	}))
	mux.HandleFunc("POST /api/queues/{name}/resume", s.require(auth.PermQueuesWrite, func(w http.ResponseWriter, r *http.Request) {
		h.ResumeQueue(w, r, r.PathValue("name"))
	}))
	mux.HandleFunc("POST /api/queues/{name}/purge", s.require(auth.PermQueuesWrite, func(w http.ResponseWriter, r *http.Request) {
		h.PurgeQueue(w, r, r.PathValue("name"))
	}))
	mux.HandleFunc("GET /api/jobs/{id}", s.require(auth.PermJobsRead, func(w http.ResponseWriter, r *http.Request) {
		h.Job(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("POST /api/jobs/{id}/retry", s.require(auth.PermJobsWrite, func(w http.ResponseWriter, r *http.Request) {
		h.RetryJob(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("POST /api/jobs/{id}/requeue", s.require(auth.PermJobsWrite, func(w http.ResponseWriter, r *http.Request) {
		h.RequeueJob(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/jobs/{id}", s.require(auth.PermJobsWrite, func(w http.ResponseWriter, r *http.Request) {
		h.DeleteJob(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/workers", s.require(auth.PermWorkersRead, h.Workers))
	mux.HandleFunc("GET /api/workers/{id}", s.require(auth.PermWorkersRead, func(w http.ResponseWriter, r *http.Request) {
		h.Worker(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/dlq", s.require(auth.PermDLQRead, h.DLQ))
	mux.HandleFunc("POST /api/dlq/purge", s.require(auth.PermDLQWrite, h.PurgeDLQ))
	mux.HandleFunc("POST /api/dlq/{id}/retry", s.require(auth.PermDLQWrite, func(w http.ResponseWriter, r *http.Request) {
		h.RetryDLQ(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/dlq/{id}", s.require(auth.PermDLQWrite, func(w http.ResponseWriter, r *http.Request) {
		h.DeleteDLQ(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/delayed", s.require(auth.PermDelayedRead, h.Delayed))
	mux.HandleFunc("POST /api/delayed/{id}/promote", s.require(auth.PermDelayedWrite, func(w http.ResponseWriter, r *http.Request) {
		h.PromoteDelayed(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/delayed/{id}", s.require(auth.PermDelayedWrite, func(w http.ResponseWriter, r *http.Request) {
		h.CancelDelayed(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/metrics/throughput", s.require(auth.PermMetricsRead, h.Throughput))

	srv := &http.Server{
		Addr: addr,
		// Outer-most middleware first: headers on every response, then CORS,
		// then a body cap, then the router.
		Handler:           securityHeaders(corsMiddleware(parseOrigins(os.Getenv("FORGEQUEUE_CORS_ORIGINS")))(limitBody(envInt64(os.Getenv("FORGEQUEUE_MAX_BODY_BYTES"), 1<<20))(mux))),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	s.srv = srv
	s.h = h
	return s
}

// loginLimiter rejects credential attempts over budget, per IP and globally.
func (s *Server) loginLimiter(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.loginGlobal.allow("global") || !s.loginIP.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "too many login attempts, try again later")
			return
		}
		next(w, r)
	}
}

// SetOIDC enables single sign-on on the server. Call before Start.
func (s *Server) SetOIDC(o *auth.OIDC) {
	s.oidc = o
	if s.h != nil {
		s.h.setOIDC(o)
	}
}

// require wraps a handler with bearer-token authentication and an optional
// permission check. An empty perm authenticates without authorizing.
func (s *Server) require(perm auth.Permission, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := s.authenticate(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if perm != "" && !id.Can(perm) {
			writeError(w, http.StatusForbidden, "insufficient permissions")
			return
		}
		next(w, r.WithContext(auth.WithIdentity(r.Context(), id)))
	}
}

// authenticate extracts and verifies the Authorization: Bearer token.
func (s *Server) authenticate(r *http.Request) (auth.Identity, bool) {
	if s.auth == nil {
		return auth.Identity{}, false
	}
	const prefix = "bearer "
	header := r.Header.Get("Authorization")
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return auth.Identity{}, false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return auth.Identity{}, false
	}
	id, err := s.auth.VerifyToken(token)
	if err != nil {
		return auth.Identity{}, false
	}
	return id, true
}

func (s *Server) Start() error {
	go func() {
		_ = s.srv.ListenAndServe()
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"message": msg})
}
