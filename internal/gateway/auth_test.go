package gateway

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"forgequeue/internal/auth"
	queuev1 "forgequeue/proto/v1"
)

// testInterceptor builds an auth Service with a fixed secret and returns the
// interceptor plus a token for the given role.
func testInterceptor(t *testing.T, role auth.Role) (grpc.UnaryServerInterceptor, string) {
	t.Helper()
	svc, err := auth.NewService(nil, []byte("test-secret"), 0)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	token, _, err := svc.IssueToken(auth.Identity{Username: "tester", Roles: []auth.Role{role}})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return newAuthInterceptor(svc), token
}

func ctxWithToken(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

// run drives the interceptor with a handler that records whether it was called.
func run(t *testing.T, ic grpc.UnaryServerInterceptor, ctx context.Context) (bool, error) {
	t.Helper()
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		if _, ok := auth.FromContext(ctx); !ok {
			t.Fatal("handler ran without an identity in context")
		}
		return "ok", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: queuev1.QueueService_EnqueueTask_FullMethodName}
	_, err := ic(ctx, nil, info, handler)
	return called, err
}

func TestAuthInterceptorRejectsMissingToken(t *testing.T) {
	ic, _ := testInterceptor(t, auth.RoleAdmin)
	called, err := run(t, ic, context.Background())
	if called {
		t.Fatal("handler ran without a token")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestAuthInterceptorRejectsBadToken(t *testing.T) {
	ic, _ := testInterceptor(t, auth.RoleAdmin)
	called, err := run(t, ic, ctxWithToken("not-a-jwt"))
	if called {
		t.Fatal("handler ran with an invalid token")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestAuthInterceptorRejectsInsufficientRole(t *testing.T) {
	// A viewer reaches the auth layer but lacks queues:write.
	ic, token := testInterceptor(t, auth.RoleViewer)
	called, err := run(t, ic, ctxWithToken(token))
	if called {
		t.Fatal("handler ran for an under-privileged identity")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestAuthInterceptorAllowsOperator(t *testing.T) {
	ic, token := testInterceptor(t, auth.RoleOperator)
	called, err := run(t, ic, ctxWithToken(token))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler did not run for an authorized identity")
	}
}

func TestAuthInterceptorDeniesUnknownMethod(t *testing.T) {
	ic, token := testInterceptor(t, auth.RoleAdmin)
	called := false
	handler := func(context.Context, any) (any, error) { called = true; return nil, nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/queue.v1.QueueService/DeleteEverything"}
	if _, err := ic(ctxWithToken(token), nil, info, handler); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied for an unmapped method", status.Code(err))
	}
	if called {
		t.Fatal("handler ran for an unmapped method")
	}
}
