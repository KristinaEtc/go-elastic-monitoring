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
	wg     sync.WaitGroup
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
	URL          string
	TemplateName string
	Remapping    bool
	ID           string
	Name         string
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

func recvMessages(b *Bulker) {

	defer func() {
		stop <- true
	}()

	var wg sync.WaitGroup
	for _, sub := range globalOpt.Subscriptions {
		wg.Add(1)
		go readFromSub(sub, &wg, b)
	}
	wg.Wait()
}

func Connect(address string, login string, passcode string) (*stomp.Conn, error) {

	options = []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(login, passcode),
		stomp.ConnOpt.Host(address),
		stomp.ConnOpt.Header("wormmq.link.peer", globalOpt.ElasticServer.ID),
		stomp.ConnOpt.Header("wormmq.link.peer_name", globalOpt.ElasticServer.Name),
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

func readFromSub(subNode Subs, wg *sync.WaitGroup, b *Bulker) {
	//var msgCount = 0
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
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)

		//check if message has necessary fields; adding fields
		if message, utc, err = formatMsg(msg.Body); err != nil {
			continue
		}

		r := elastic.NewBulkIndexRequest().
			Index(subNode.Index + "-" + *utc).
			Type(subNode.TypeName).
			Doc(*message)

		if r == nil {
			log.Warn("nil")
			os.Exit(1)
		}

		//processor.Add(r)
		b.p.Add(r)

		//	log.Debug("4")

		//log.Infof("[%s]/[%s]", subNode.Queue, subNode.Index)
		/*if subNode.Index == "global_logs" {
			_, err = client.Index().
				Index(subNode.Index + "-" + *utc).
				Type(globalOpt.TypeName).
				BodyString(*message).
				//Refresh(true).
				Do()
			if err != nil {
				log.Errorf("Elasticsearch [index %s]: %s in message %s", subNode.Index, err.Error(), *message)
				//log.Errorf("Elasticsearch: %s", err.Error())
				os.Exit(1)
			}
			//time.Sleep(time.Second)
			msgCount++
		}*/
	}
}

func main() {

	var err error
	config.ReadGlobalConfig(&globalOpt, "go-elastic-monitoring options")
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

	if globalOpt.ElasticServer.Remapping {
		prepareElasticIndexTemplate()
	}

	b := configurateBulkProcess()
	defer b.Close()

	go recvMessages(b)
	<-stop
}

func configurateBulkProcess() *Bulker {
	b := &Bulker{c: client, index: "global_logs-2"}
	err := b.Run()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
	//defer b.Close()
	// Run the statistics printer
	go func(b *Bulker) {
		for range time.Tick(10 * time.Second) {
			printStats(b)
		}
	}(b)

	return b
}

func prepareElasticIndexTemplate() {

	mappedTempl, err := initTemplate(globalOpt.ElasticServer.TemplateName)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	//	log.Info(mappedTempl)

	//template := strings.Replace(mappingTemplate, "%%MAPPING_VERSION%%", mappingVersion, -1)
	_, err = client.IndexPutTemplate(globalOpt.ElasticServer.TemplateName).BodyString(mappedTempl).Do()
	if err != nil {
		log.Errorf("Could not add index template; %s", err.Error())
		os.Exit(1)
	} else {
		log.Info("Index template added.")
	}
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
		log.Errorf("Could not get parse request: %s", err.Error())
		return nil, err
	}
	if data.ID == "" || data.Type == "" || data.Utc == "" {
		log.Warn("At least one of that field [ID, Type, Utc] not found; message ignored")
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
