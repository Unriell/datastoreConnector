package connector

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"

	"cloud.google.com/go/datastore"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type BasicCounter struct {
	Amount int
}

type datastoreAtomicConnector struct {
	client         *datastore.Client
	ctx            context.Context
	CollectionName string
}

type DatastoreAtomicOpt interface {
	Count(entityID string) int
	DecrementCounter(entityID string, decrementAmount int) bool
	IncrementCounter(entityID string, incrementAmount int) bool
}

// NewAtomicConnector is a factory method that create new datastoreAtomicConnector single instances. This connector run all operations in transaction mode.
// Transaction represents a set of datastore operations to be committed atomically.
//
// Operations are enqueued by calling the Put and Delete methods on Transaction
// (or their Multi-equivalents).  These operations are only committed when the
// Commit method is invoked. To ensure consistency, reads must be performed by
// using Transaction's Get method or by using the Transaction method when
// building a query.
func NewAtomicConnector(emulatorEnable bool, datastoreEmulatorAddr string, gcloudCredentialsPath, projectID, CollectionName string) DatastoreAtomicOpt {
	var Instance = new(datastoreAtomicConnector)
	Instance.CollectionName = CollectionName
	Instance.ctx = context.Background()
	var err error
	switch getClientType(emulatorEnable, gcloudCredentialsPath) {
	case EMULATOR:
		os.Setenv("DATASTORE_EMULATOR_HOST", datastoreEmulatorAddr)
		if Instance.client, err = datastore.NewClient(Instance.ctx, projectID); err != nil {
			log.Fatal(err)
		}

		break
	case SIMPLE:
		client, err := datastore.NewClient(Instance.ctx, projectID)

		if err != nil {
			log.Fatal(err)
		}

		Instance.client = client
		break
	case KEYFILE:

		jsonKey, err := ioutil.ReadFile(path.Join(gcloudCredentialsPath, "keyfile.json"))

		if err != nil {
			log.Fatal(err)
		}

		conf, err := google.JWTConfigFromJSON(
			jsonKey,
			datastore.ScopeDatastore,
		)

		if err != nil {
			log.Fatal(err)
		}

		client, err := datastore.NewClient(
			Instance.ctx,
			projectID,
			option.WithTokenSource(conf.TokenSource(Instance.ctx)),
		)

		if err != nil {
			log.Fatal(err)
		}

		Instance.client = client
		break
	default:
		log.Fatal("Unknown Datastore client")
		break
	}

	return Instance
}

func (d *datastoreAtomicConnector) IncrementCounter(entityID string, incrementAmount int) (success bool) {

	t, err := d.client.NewTransaction(d.ctx)

	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	var counter BasicCounter
	err = t.Get(inboundKey, &counter)
	if err == nil || err == datastore.ErrNoSuchEntity {
		counter.Amount = counter.Amount + incrementAmount
		_, err = t.Put(inboundKey, &counter)
		_, err = t.Commit()
	}

	if err == nil {
		success = true
	}

	return
}

func (d *datastoreAtomicConnector) DecrementCounter(entityID string, decrementAmount int) (success bool) {
	t, err := d.client.NewTransaction(d.ctx)

	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	var counter BasicCounter
	err = t.Get(inboundKey, &counter)
	if err == nil || err == datastore.ErrNoSuchEntity {
		counter.Amount = counter.Amount - decrementAmount
		if counter.Amount < 0 {
			counter.Amount = 0
		}
		_, err = t.Put(inboundKey, &counter)
		_, err = t.Commit()
	}

	if err == nil {
		success = true
	}

	return
}

func (d *datastoreAtomicConnector) Count(entityID string) (amount int) {
	t, err := d.client.NewTransaction(d.ctx)

	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	var counter BasicCounter
	err = t.Get(inboundKey, &counter)
	_, err = t.Commit()
	if err != nil {
		amount = 0
	}

	if err == nil {
		amount = counter.Amount
	}

	return
}
