package persistence_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
)

func newNamespaceDetailBlob(t *testing.T, serializer serialization.Serializer, name string) *commonpb.DataBlob {
	t.Helper()
	blob, err := serializer.NamespaceDetailToBlob(&persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   "namespace-id",
			Name: name,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "test-cluster",
			Clusters:          []string{"test-cluster"},
		},
	})
	require.NoError(t, err)
	return blob
}

func TestMetadataManagerRenameNamespaceIdempotentWhenAlreadyRenamed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mock.NewMockMetadataStore(ctrl)
	serializer := serialization.NewSerializer()
	manager := persistence.NewMetadataManagerImpl(store, serializer, log.NewNoopLogger(), "test-cluster")

	newName := "namespace-deleted-names"
	store.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "namespace"}).
		Return(nil, serviceerror.NewNamespaceNotFound("namespace"))
	store.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: newName}).
		Return(&persistence.InternalGetNamespaceResponse{
			Namespace: newNamespaceDetailBlob(t, serializer, newName),
		}, nil)

	// A retried rename whose first attempt committed before its response was lost must succeed without a
	// second conditional write.
	err := manager.RenameNamespace(context.Background(), &persistence.RenameNamespaceRequest{
		PreviousName: "namespace",
		NewName:      newName,
	})
	require.NoError(t, err)
}

func TestMetadataManagerRenameNamespaceReturnsErrorWhenNeitherNameExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mock.NewMockMetadataStore(ctrl)
	manager := persistence.NewMetadataManagerImpl(
		store, serialization.NewSerializer(), log.NewNoopLogger(), "test-cluster")

	store.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "namespace"}).
		Return(nil, serviceerror.NewNamespaceNotFound("namespace"))
	store.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "namespace-deleted-names"}).
		Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-names"))

	err := manager.RenameNamespace(context.Background(), &persistence.RenameNamespaceRequest{
		PreviousName: "namespace",
		NewName:      "namespace-deleted-names",
	})
	var notFound *serviceerror.NamespaceNotFound
	require.ErrorAs(t, err, &notFound)
}
