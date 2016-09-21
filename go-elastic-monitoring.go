package main

import (
	"encoding/json"
	"strconv"
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
	Index    string
}

// GlobalConf is a struct with global options,
// like server address and queue format, etc.

type ConfigFile struct {
	Subscriptions []Subs
	TypeName      string
}

var globalOpt = ConfigFile{
	Subscriptions: []Subs{},
	TypeName:      "table-info",
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
	var msgCount = 0
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
			//log.Warn("Got empty message; ignore")
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)

		//check if message has necessary fields; adding fields
		if message, utc, err = formatMsg(msg.Body); err != nil {
			continue
		}

		log.Infof("[%s]/[%s]", subNode.Queue, subNode.Index)
		_, err = client.Index().
			Index(subNode.Index + *(utc)).
			Type(globalOpt.TypeName).
			Id(strconv.Itoa(msgCount)).
			BodyJson(message).
			Refresh(true).
			Do()
		if err != nil {
			//log.Errorf("Elasticsearch: %s in message %s", err.Error(), message)
			log.Errorf("Elasticsearch: %s", err.Error())
		}
		time.Sleep(time.Second)
		msgCount++

		//client.IndexPutTemplate(subNode.Index()).BodyString
	}
}

func main() {

	var err error
	config.ReadGlobalConfig(&globalOpt, "go-elastic-monitoring options")
	client, err = elastic.NewClient()
	if err != nil {
		log.Error("elasicsearch: could not create client")
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

	t, err := time.Parse((*importantFields).Utc, "20060102")
	if err != nil {
		log.Warnf("Could not parse utc from msg: %s", err.Error())
		return nil, nil, err
	}

	tStr := t.String()
	formattedMsg := addProcessNameShort(msg, (*importantFields).ProcessName)

	return &formattedMsg, &tStr, nil
}

func checkMsgForValid(msg []byte) (*NecessatyFields, error) {

	var data NecessatyFields

	if err := json.Unmarshal(msg, &data); err != nil {
		log.Errorf("Could not get parse request: %s", err.Error())
		return nil, err
	}
	if data.ID == "" || data.Type == "" || data.Utc == "" {
		log.Debug("No one of that field: ID, Type, Utc in message")
		return nil, ErrNoNeedfullFields
	}
	return &data, nil
}

func addProcessNameShort(msg []byte, fullProcessName string) string {

	log.Info("****************************")

	shortName := getShortName(fullProcessName)
	newMsg := "{\"process_short\": \"" + shortName + "\", " + string(msg[1:])
	log.Info(newMsg)
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
	log.Infof("processNameShort /=%s", shortName)

	shortName = trimStringFromSym(fullName, "\\")
	if shortName != "" {
		return shortName
	}
	log.Infof("processNameShort \\=%s", shortName)

	log.Debug("processShort = process")
	return fullName
}

func trimStringFromSym(str string, sym string) string {
	if idx := strings.LastIndex(str, sym); idx != -1 {
		return str[(idx - 1):]
	}
	return str
}
