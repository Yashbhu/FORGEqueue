package gateway

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"forgequeue/internal/auth"
	queuev1 "forgequeue/proto/v1"
)

// methodPermissions maps each RPC to the permission it requires. A method
// that is not listed is denied, so adding an RPC without granting it here
// fails closed rather than silently exposing it.
var methodPermissions = map[string]auth.Permission{
	queuev1.QueueService_EnqueueTask_FullMethodName: auth.PermQueuesWrite,
}

// newAuthInterceptor authenticates every unary call with the same bearer
// tokens the HTTP API issues, then authorizes the method's required
// permission. Identity is attached to the handler context for downstream use.
func newAuthInterceptor(svc *auth.Service) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		token, err := bearerToken(ctx)
		if err != nil {
			return nil, err
		}

		id, err := svc.VerifyToken(token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid or missing credentials")
		}

		required, ok := methodPermissions[info.FullMethod]
		if !ok {
			return nil, status.Errorf(codes.PermissionDenied, "no policy for method %s", info.FullMethod)
		}
		if !id.Can(required) {
			return nil, status.Errorf(codes.PermissionDenied, "missing permission %s", required)
		}

		return handler(auth.WithIdentity(ctx, id), req)
	}
}

// bearerToken extracts the token from the gRPC "authorization" metadata entry.
func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing request metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	fields := strings.Fields(vals[0])
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bearer") {
		return "", status.Error(codes.Unauthenticated, "malformed authorization metadata")
	}
	return fields[1], nil
}
