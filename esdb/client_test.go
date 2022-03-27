package esdb_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
	"github.com/stretchr/testify/assert"
)

func TestSingleNode(t *testing.T) {
	// Empty database container
	t.Log("[debug] starting empty database container...")
	emptyContainer := GetEmptyDatabase(t)
	t.Cleanup(emptyContainer.Close)
	emptyContainerClient := CreateTestClient(emptyContainer, t)
	WaitForAdminToBeAvailable(t, emptyContainerClient)
	t.Cleanup(func() { emptyContainerClient.Close() })
	t.Log("[debug] empty database container started and ready to serve!")
	//

	// Prepopulated database container
	t.Log("[debug] starting prepopulated database container...")
	populatedContainer := GetPrePopulatedDatabase(t)
	t.Cleanup(populatedContainer.Close)
	populatedContainerClient := CreateTestClient(populatedContainer, t)
	WaitForAdminToBeAvailable(t, populatedContainerClient)
	t.Cleanup(func() { populatedContainerClient.Close() })
	t.Log("[debug] prepopulated database container started and ready to serve!")
	//

	// Those ReadAll tests need to be executed first because those are based on $all specific order.
	ReadAllTests(t, populatedContainerClient)
	ReadStreamTests(t, emptyContainerClient, populatedContainerClient)
	SubscriptionTests(t, populatedContainerClient)
	AppendTests(t, emptyContainer, emptyContainerClient)
	ConnectionTests(t, emptyContainer)
	DeleteTests(t, emptyContainerClient)
	PersistentSubReadTests(t, emptyContainerClient)
	PersistentSubTests(t, emptyContainerClient, populatedContainerClient)
	TLSTests(t, emptyContainer)

	Closing(t, emptyContainerClient)
}

func TestClusterNode(t *testing.T) {
	db := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=leader&tlsverifycert=false", t)

	WaitForAdminToBeAvailable(t, db)
	WaitForLeaderToBeElected(t, db)

	t.Cleanup(func() { db.Close() })

	ClusterTests(t)
	ReadStreamTests(t, db, nil)
	AppendTests(t, nil, db)
	DeleteTests(t, db)
	PersistentSubReadTests(t, db)
	PersistentSubTests(t, db, nil)
}

func Closing(t *testing.T, client *esdb.Client) {
	client.Close()

	_, err := client.AppendToStream(context.Background(), NAME_GENERATOR.Generate(), esdb.AppendToStreamOptions{}, createTestEvent())

	esdbErr, _ := esdb.FromError(err)
	assert.Error(t, esdbErr)
	assert.Equal(t, esdb.ErrorConnectionClosed, esdbErr.Code())
}
