package deletenamespace

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func Test_GetNamespaceInfoActivityUsesReplicationResolver(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace"},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "other-cluster",
			Clusters:          []string{"current-cluster", "other-cluster"},
		},
	}

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace",
	}).Return(&persistence.GetNamespaceResponse{Namespace: detail}, nil)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("current-cluster")

	a := &localActivities{
		metadataManager: metadataManager,
		clusterMetadata: clusterMetadata,
		replicationResolverFactory: func(input *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
			require.Same(t, detail, input)
			return namespace.NewDefaultReplicationResolverFactory()(&persistencespb.NamespaceDetail{
				FailoverVersion: 1,
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: "other-cluster",
					Clusters:          []string{"other-cluster"},
				},
			})
		},
	}

	result, err := a.GetNamespaceInfoActivity(context.Background(), namespace.EmptyID, "namespace")
	require.NoError(t, err)
	require.Equal(t, getNamespaceInfoResult{
		NamespaceID:                 "namespace-id",
		Namespace:                   "namespace",
		Clusters:                    []string{"other-cluster"},
		ActiveCluster:               "other-cluster",
		CurrentCluster:              "current-cluster",
		CanDeleteFromCurrentCluster: true,
	}, result)
}

func Test_MarkNamespaceDeletedActivity_IdempotentWhenAlreadyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    "namespace-id",
			Name:  "namespace",
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	}

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace",
	}).Return(&persistence.GetNamespaceResponse{Namespace: detail}, nil)

	a := &localActivities{
		metadataManager: metadataManager,
		logger:          log.NewTestLogger(),
	}

	require.NoError(t, a.MarkNamespaceDeletedActivity(context.Background(), "namespace"))
}

func Test_MarkNamespaceDeletedActivity_MarksRegisteredNamespaceDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    "namespace-id",
			Name:  "namespace",
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
	}

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace",
	}).Return(&persistence.GetNamespaceResponse{Namespace: detail}, nil)
	metadataManager.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: 10,
	}, nil)
	metadataManager.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			require.Equal(t, enumspb.NAMESPACE_STATE_DELETED, request.Namespace.Info.State)
			require.Equal(t, int64(10), request.NotificationVersion)
			return nil
		},
	)

	a := &localActivities{
		metadataManager: metadataManager,
		logger:          log.NewTestLogger(),
	}

	require.NoError(t, a.MarkNamespaceDeletedActivity(context.Background(), "namespace"))
}

func Test_GenerateDeletedNamespaceNameActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)

	a := &localActivities{
		metadataManager: metadataManager,
		logger:          log.NewTestLogger(),
	}

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-names",
	}).Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-names"))
	deletedName, err := a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-names"), deletedName)

	deletedName, err = a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace-deleted-names")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-names"), deletedName)

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-names",
	}).Return(nil, nil)
	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-namesp",
	}).Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-namesp"))
	deletedName, err = a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-namesp"), deletedName)

	ctrl.Finish()
}
func Test_ValidateNexusEndpointsActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	nexusEndpointManager := persistence.NewMockNexusEndpointManager(ctrl)

	a := &localActivities{
		nexusEndpointManager: nexusEndpointManager,
		logger:               log.NewTestLogger(),

		allowDeleteNamespaceIfNexusEndpointTarget: func() bool { return false },
		nexusEndpointListDefaultPageSize:          func() int { return 100 },
	}

	// The "fake" namespace ID is associated with a Nexus endoint.
	nexusEndpointManager.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusEndpointsResponse{
		Entries: []*persistencespb.NexusEndpointEntry{
			{
				Endpoint: &persistencespb.NexusEndpoint{
					Spec: &persistencespb.NexusEndpointSpec{
						Name: "test-endpoint",
						Target: &persistencespb.NexusEndpointTarget{
							Variant: &persistencespb.NexusEndpointTarget_Worker_{
								Worker: &persistencespb.NexusEndpointTarget_Worker{
									NamespaceId: "namespace-id",
								},
							},
						},
					},
				},
			},
		},
	}, nil).Times(2)

	err := a.ValidateNexusEndpointsActivity(context.Background(), "namespace-id", "namespace")
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)

	err = a.ValidateNexusEndpointsActivity(context.Background(), "namespace2-id", "namespace2")
	require.NoError(t, err)

	nexusEndpointManager.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(nil, errors.New("persistence failure"))
	err = a.ValidateNexusEndpointsActivity(context.Background(), "namespace-id", "namespace")
	require.Error(t, err)
	require.Equal(t, "unable to list Nexus endpoints for namespace namespace: persistence failure", err.Error())

	ctrl.Finish()
}
