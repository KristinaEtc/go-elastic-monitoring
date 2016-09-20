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
	//defer func() {
	//	stop <- true
	//}()
	//heartbeat := time.Duration(globalOpt.Global.Heartbeat) * time.Second

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

	for {
		msg, err := sub.Read()
		if err != nil {
			//log.Warn("Got empty message; ignore")
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
		message, send := addProcessNameShort(msg.Body)
		if !send {
			//trash data; not redirect to elastic
			continue
		}

		log.Infof("[%s]/[%s]", subNode.Queue, subNode.Index)
		_, err = client.Index().
			Index(subNode.Index).
			Type("external").
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

func addProcessNameShort(msg []byte) (string, bool) {

	log.Info("****************************")
	type strWithProcessName struct {
		ProcessName string `json:"process"`
		Id          string `json:"id"`
		Type        string `json:"type"`
		Utc         string `json:"utc"`
	}

	var processNameShort = ""
	var b strWithProcessName
	if err := json.Unmarshal(msg, &b); err != nil {
		log.Errorf("Could not get parse request: %s", err.Error())
		return string(msg), false
	} else {
		if b.Id == "" || b.Type == "" || b.Utc == "" {
			log.Debug("[b.Id  || b.Type  || b.Utc] == nil ")
			return string(msg), false
		}
		log.Infof("[process]=[%s]", b.ProcessName)
		processNameShort = trimStringFromSym(b.ProcessName, "/")

		log.Infof("processNameShort /=%s", processNameShort)
		if processNameShort == "" {
			processNameShort = trimStringFromSym(b.ProcessName, "\\")
			log.Infof("processNameShort \\=%s", processNameShort)
		}
		if processNameShort == "" {
			log.Infof("processNameShort =%s", processNameShort)
			if b.ProcessName == "" {
				log.Infof("processNameShort b.ProcessName =%s", processNameShort)
				b.ProcessName = "nil"
			}
			processNameShort = b.ProcessName
		}
		log.Infof("[process_short]=%s", processNameShort)
		newMsg := "{\"process_short\": \"" + processNameShort + "\", " + string(msg[1:])

		log.Info(newMsg)
		return newMsg, true
	}

}

func trimStringFromSym(str string, sym string) string {
	if idx := strings.Index(str, sym); idx != -1 {
		return str[(idx - 1):]
	}
	return str
}
