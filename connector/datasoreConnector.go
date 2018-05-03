package connector

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

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
	client *datastore.Client
	ctx    context.Context
}

// DatastoreBasicOpt represents datastore basic operations as CRUD methods
type DatastoreBasicOpt interface {
	Save(inboundKey *datastore.Key, entity interface{}) (*datastore.Key, error)
	Exist(key *datastore.Key) bool
	Delete(key *datastore.Key) bool
}

var once sync.Once

// Instance represent a single dao instance.
var Instance *datastoreConnector

// New is a factory method that create new datastore connector single instances
func New(emulatorEnable bool, datastoreEmulatorAddr string, gcloudCredentialsPath, projectID string) DatastoreBasicOpt {
	once.Do(func() {
		Instance = new(datastoreConnector)
		Instance.ctx = context.Background()
		var err error
		switch getClientType(emulatorEnable, gcloudCredentialsPath) {
		case EMULATOR:
			if Instance.client, err = datastore.NewClient(Instance.ctx, projectID); err != nil {
				log.Fatal(err)
			}
			os.Setenv("DATASTORE_EMULATOR_HOST", datastoreEmulatorAddr)
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

	})

	return Instance
}

func (d *datastoreConnector) Save(inboundKey *datastore.Key, entity interface{}) (key *datastore.Key, err error) {
	key, err = d.client.Put(d.ctx, inboundKey, entity)
	return
}

func (d *datastoreConnector) Exist(key *datastore.Key) (exist bool) {

	exist = false
	if amount, err := d.client.Count(d.ctx, datastore.NewQuery("application").KeysOnly().Filter("__key__ =", key)); err == nil {
		if amount > 0 {
			exist = true
		}
	}
	return
}

func (d *datastoreConnector) Delete(key *datastore.Key) (deleted bool) {

	if err := d.client.Delete(d.ctx, key); err != nil {
		deleted = true
	}

	return
}
