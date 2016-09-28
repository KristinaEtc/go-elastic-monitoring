package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	//important: must execute first; do not move
	_ "github.com/KristinaEtc/slflog"

	"github.com/KristinaEtc/config"
	"github.com/go-stomp/stomp"
	_ "github.com/lib/pq"
	"github.com/ventu-io/slf"
	elastic "gopkg.in/olivere/elastic.v3"
)

var log = slf.WithContext("go-stomp-nominatim.go")

var options = []func(*stomp.Conn) error{}

var (
	stop   = make(chan bool)
	client *elastic.Client
)

var (
	// These fields are populated by govvv
	BuildDate  string
	GitCommit  string
	GitBranch  string
	GitState   string
	GitSummary string
	Version    string
)

type Subs struct {
	Host     string
	Login    string
	Passcode string
	Queue    string
	TypeName string
	Index    string
}

type Elastic struct {
	URL string
}

// GlobalConf is a struct with global options,
// like server address and queue format, etc.

type ConfigFile struct {
	Subscriptions []Subs
	ElasticServer Elastic `json:"Elastic"`
}

var globalOpt = ConfigFile{
	Subscriptions: []Subs{},
}

func recvMessages() {

	defer func() {
		stop <- true
	}()

	var wg sync.WaitGroup
	for _, sub := range globalOpt.Subscriptions {
		wg.Add(1)
		go readFromSub(sub, &wg)
	}
	wg.Wait()
}

func Connect(address string, login string, passcode string) (*stomp.Conn, error) {

	options = []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(login, passcode),
		stomp.ConnOpt.Host(address),
		//stomp.ConnOpt.HeartBeat(heartbeat, heartbeat),
	}

	log.Infof("Connecting to %s: [%s, %s]\n", address, login, passcode)

	conn, err := stomp.Dial("tcp", address, options...)
	if err != nil {
		log.Errorf("cannot connect to server %s: %v ", address, err.Error())
		return nil, err
	}

	return conn, nil
}

func readFromSub(subNode Subs, wg *sync.WaitGroup) {
	defer wg.Done()

	log.WithFields(slf.Fields{
		"queque":       subNode.Queue,
		"elasticIndex": subNode.Index,
	}).Debug("Configuring... ")

	conn, err := Connect(subNode.Host, subNode.Login, subNode.Passcode)
	if err != nil {
		return
	}
	log.Infof("Subscribing to %s", subNode.Queue)
	sub, err := conn.Subscribe(subNode.Queue, stomp.AckAuto)
	if err != nil {
		log.Errorf("Cannot subscribe to %s: %v", subNode.Queue, err.Error())
		return
	}

	var message *string
	var utc *string

	for {
		msg, err := sub.Read()
		if err != nil {
			log.Warn("Got empty message; ignore")
			continue
		}

		//check if message has necessary fields; adding fields
		if message, utc, err = formatMsg(msg.Body); err != nil {
			log.Errorf("topic %s - err %s", subNode.Queue, err.Error())
			continue
		}

		//log.Infof("[%s]/[%s]", subNode.Queue, subNode.Index)
		_, err = client.Index().
			Index(subNode.Index + "-" + *utc).
			Type(subNode.TypeName).
			BodyString(*message).
			//Refresh(true).
			Do()
		if err != nil {
			log.Errorf("Elasticsearch [index %s][topic %s]: %s in message %s", subNode.Index, subNode.Queue, err.Error(), *message)
			//log.Errorf("Elasticsearch: %s", err.Error())
			os.Exit(1)
		}
	}
}

func main() {

	var err error
	config.ReadGlobalConfig(&globalOpt, "go-elastic-monitoring options")
	log.Debug(globalOpt.ElasticServer.URL)
	client, err = elastic.NewClient(elastic.SetURL(globalOpt.ElasticServer.URL))
	if err != nil {
		log.Error("elasicsearch: could not create client")
		os.Exit(1)
	}

	log.Infof("BuildDate=%s\n", BuildDate)
	log.Infof("GitCommit=%s\n", GitCommit)
	log.Infof("GitBranch=%s\n", GitBranch)
	log.Infof("GitState=%s\n", GitState)
	log.Infof("GitSummary=%s\n", GitSummary)
	log.Infof("VERSION=%s\n", Version)

	log.Info("Starting working...")

	go recvMessages()
	<-stop
}

func formatMsg(msg []byte) (*string, *string, error) {

	// unmarshal check if msg has nessesary fields
	importantFields, err := checkMsgForValid(msg)
	if err != nil {
		return nil, nil, err
	}

	lenUTCFormat := len((*importantFields).Utc)
	var timeStr string
	var t time.Time

	switch {
	case lenUTCFormat == lenRfc3339:
		{
			t, err = time.Parse(time.RFC3339, (*importantFields).Utc)
		}
	case lenUTCFormat < lenRfc3339:
		{
			if lenUTCFormat == lenRfc3339WithoutZ {
				t, err = time.Parse("2006-01-02T15:04:05", (*importantFields).Utc)
			}
			if lenUTCFormat == lenRfc3339WithZ {
				t, err = time.Parse("2006-01-02T15:04:05Z", (*importantFields).Utc)
			}
		}
	case lenUTCFormat > lenRfc3339:
		{
			if lenUTCFormat == lenRfc3339WithTimeZone {
				t, err = time.Parse("2006-01-02T15:04:05Z +0300 MSK", (*importantFields).Utc)
			}
		}
	default:
		{
			log.Warnf("Unknown utc format: %s", (*importantFields).Utc)
			err = ErrUnknownUTCFormat
		}
	}

	if err != nil {
		return nil, nil, err
	}

	timeStr = t.String()

	//cut all except date
	idx := strings.IndexAny(timeStr, " ")
	timeStr = timeStr[:idx]

	formattedMsg := addProcessNameShort(msg, (*importantFields).ProcessName)

	return &formattedMsg, &timeStr, nil
}

func checkMsgForValid(msg []byte) (*NecessaryFields, error) {

	var data NecessaryFields

	if err := json.Unmarshal(msg, &data); err != nil {
		//log.Errorf("Could not get parse request: %s", err.Error())
		return nil, err
	}
	if data.ID == "" || data.Type == "" || data.Utc == "" {
		//log.Warn("At least one of that field [ID, Type, Utc] not found; message ignored")
		return nil, ErrNoNeedfullFields
	}
	return &data, nil
}

func addProcessNameShort(msg []byte, fullProcessName string) string {

	shortName := getShortName(fullProcessName)
	newMsg := "{\"process_short\": \"" + shortName + "\", " + string(msg[1:])
	return newMsg
}

func getShortName(fullName string) string {
	if fullName == "" {
		return ""
	}

	var shortName string

	shortName = trimStringFromSym(fullName, "/")
	if shortName != "" {
		return shortName
	}

	shortName = trimStringFromSym(fullName, "\\")
	if shortName != "" {
		return shortName
	}

	return fullName
}

func trimStringFromSym(str string, sym string) string {
	if idx := strings.LastIndex(str, sym); idx != -1 {
		return str[(idx-1)+2:]
	}
	return ""
}

func prepareElasticIndexTemplate() {

	//template := strings.Replace(mappingTemplate, "%%MAPPING_VERSION%%", mappingVersion, -1)
	_, err := client.IndexPutTemplate("global_logs-2*").BodyString(mappingTemplate).Do()
	if err != nil {
		log.Errorf("Could not add index template; %s", err.Error())
		os.Exit(1)
	} else {
		log.Info("Index template added.")
	}
}
