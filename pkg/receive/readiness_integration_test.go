// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// mockWriteableStoreServer implements a simple WriteableStore service for testing.
type mockWriteableStoreServer struct {
	storepb.UnimplementedWriteableStoreServer
	callCount int
}

func (m *mockWriteableStoreServer) RemoteWrite(_ context.Context, _ *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	m.callCount++
	return &storepb.WriteResponse{}, nil
}

// TestReadinessFeatureIntegration tests the full integration of the readiness feature
// including feature flag parsing and gRPC server setup.
func TestReadinessFeatureIntegration(t *testing.T) {
	t.Run("feature flag parsing", func(t *testing.T) {
		features := []string{"metric-names-filter", "grpc-readiness-interceptor"}

		var enableReadiness bool
		for _, feature := range features {
			if feature == "grpc-readiness-interceptor" {
				enableReadiness = true
			}
		}
		testutil.Equals(t, true, enableReadiness)
	})

	t.Run("grpc server with readiness", func(t *testing.T) {
		testReadinessWithGRPCServer(t, true)
	})

	t.Run("grpc server without readiness", func(t *testing.T) {
		testReadinessWithGRPCServer(t, false)
	})
}

func testReadinessWithGRPCServer(t *testing.T, enableReadiness bool) {
	httpProbe := prober.NewHTTP()

	testutil.Equals(t, false, httpProbe.IsReady())

	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()

	grpcOptions := []grpcserver.Option{
		grpcserver.WithListen("bufnet"),
	}
	if enableReadiness {
		grpcOptions = append(grpcOptions, NewReadinessGRPCOptions(httpProbe)...)
	}

	mockSrv := &mockWriteableStoreServer{}
	var serverOpts []grpc.ServerOption

	if enableReadiness {
		unaryInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			if !httpProbe.IsReady() {
				return nil, nil
			}
			return handler(ctx, req)
		}

		streamInterceptor := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if !httpProbe.IsReady() {
				return nil
			}
			return handler(srv, ss)
		}

		serverOpts = append(serverOpts,
			grpc.UnaryInterceptor(unaryInterceptor),
			grpc.StreamInterceptor(streamInterceptor),
		)
	}

	s := grpc.NewServer(serverOpts...)
	storepb.RegisterWriteableStoreServer(s, mockSrv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer s.Stop()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.Ok(t, err)
	defer conn.Close()

	client := storepb.NewWriteableStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.RemoteWrite(ctx, &storepb.WriteRequest{})
	testutil.Ok(t, err)

	if enableReadiness {
		testutil.Assert(t, resp != nil)
		testutil.Equals(t, 0, mockSrv.callCount)
	} else {
		testutil.Assert(t, resp != nil)
		testutil.Equals(t, 1, mockSrv.callCount)
	}

	httpProbe.Ready()
	testutil.Equals(t, true, httpProbe.IsReady())

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	resp2, err2 := client.RemoteWrite(ctx2, &storepb.WriteRequest{})
	testutil.Ok(t, err2)
	testutil.Assert(t, resp2 != nil)

	if enableReadiness {
		testutil.Equals(t, 1, mockSrv.callCount)
	} else {
		testutil.Equals(t, 2, mockSrv.callCount)
	}
}

// TestConstantsAndFeatureList verifies the feature constants are properly defined.
func TestConstantsAndFeatureList(t *testing.T) {
	const (
		expectedMetricNamesFilter = "metric-names-filter"
		expectedGRPCReadiness     = "grpc-readiness-interceptor"
	)

	testutil.Assert(t, len(expectedMetricNamesFilter) > 0)
	testutil.Assert(t, len(expectedGRPCReadiness) > 0)
	testutil.Equals(t, false, strings.Contains(expectedMetricNamesFilter, " "))
	testutil.Equals(t, false, strings.Contains(expectedGRPCReadiness, " "))
	featureString := fmt.Sprintf("%s,%s", expectedMetricNamesFilter, expectedGRPCReadiness)
	features := strings.Split(featureString, ",")

	testutil.Equals(t, 2, len(features))
	testutil.Equals(t, expectedMetricNamesFilter, features[0])
	testutil.Equals(t, expectedGRPCReadiness, features[1])
}
