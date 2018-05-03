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

type datatoreClientType int

const (
	// SIMPLE ...
	SIMPLE datatoreClientType = 1 + iota
	// EMULATOR ...
	EMULATOR
	// KEYFILE ...
	KEYFILE
)

var clientType = [...]string{
	"SIMPLE",
	"EMULATOR",
	"KEYFILE",
}

func (c datatoreClientType) String() string {
	return clientType[c-1]
}

func getClientType(emulatorEnable bool, gcloudCredentialsPath string) (clientType datatoreClientType) {
	if emulatorEnable {
		clientType = EMULATOR
	} else {
		if gcloudCredentialsPath != "" {
			clientType = KEYFILE
		} else {
			clientType = SIMPLE
		}
	}
	return
}

type datastoreConnector struct {
	client         *datastore.Client
	ctx            context.Context
	CollectionName string
}

// DatastoreBasicOpt represents datastore basic operations as CRUD methods
type DatastoreBasicOpt interface {
	Save(entityID string, entity interface{}) (*datastore.Key, error)
	Exist(query *datastore.Query) bool
	Delete(entityID string) bool
	Update(entityID string, entity interface{}) (*datastore.Key, error)
	Retrieve(entityID string, dst interface{}) error
}

// New is a factory method that create new datastore connector single instances
func New(emulatorEnable bool, datastoreEmulatorAddr string, gcloudCredentialsPath, projectID, CollectionName string) DatastoreBasicOpt {
	var Instance = new(datastoreConnector)
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

func (d *datastoreConnector) Save(entityID string, entity interface{}) (key *datastore.Key, err error) {
	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	key, err = d.client.Put(d.ctx, inboundKey, entity)
	return
}

func (d *datastoreConnector) Exist(query *datastore.Query) (exist bool) {
	exist = false
	if amount, err := d.client.Count(d.ctx, query); err == nil {
		if amount > 0 {
			exist = true
		}
	}
	return
}

func (d *datastoreConnector) Delete(entityID string) (deleted bool) {
	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	if err := d.client.Delete(d.ctx, inboundKey); err != nil {
		deleted = true
	}

	return
}

func (d *datastoreConnector) Update(entityID string, entity interface{}) (key *datastore.Key, err error) {
	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	key, err = d.client.Put(d.ctx, inboundKey, entity)
	return
}

func (d *datastoreConnector) Retrieve(entityID string, dst interface{}) (err error) {
	inboundKey := datastore.NameKey(d.CollectionName, entityID, nil)
	err = d.client.Get(d.ctx, inboundKey, dst)
	return
}
