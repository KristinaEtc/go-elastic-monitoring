package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
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

	processor *elastic.BulkProcessor
	instance  int
	stopping  bool
	wg        sync.WaitGroup
	//	stopping bool
	//	sigexit bool
	//	exitchan chan bool
	//	responsetime *utils.ConcurrentIntMap
)

const (
	TOO_MANY_REQUESTS     = 429
	INTERNAL_SERVER_ERROR = 500
	SERVICE_UNAVAILABLE   = 503
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

		if stopping {
			continue
		}

		//check if message has necessary fields; adding fields
		if message, utc, err = formatMsg(msg.Body); err != nil {
			continue
		}

		r := elastic.NewBulkIndexRequest().
			Index(subNode.Index + "-" + *utc).
			Type(globalOpt.TypeName).
			Doc(*message)

		if r == nil {
			log.Warn("nil")
			os.Exit(1)
		}

		processor.Add(r)

		log.Debug("4")

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

func init() {
	stopping = false
	rand.Seed(time.Now().Unix())
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

	configurateElastic()

	go recvMessages()
	<-stop
}

func configurateElastic() {
	//prepareElasticIndexTemplate()
	configurateBulkProcess()

}

func prepareElasticIndexTemplate() {

	//template := strings.Replace(mappingTemplate, "%%MAPPING_VERSION%%", mappingVersion, -1)
	_, err := client.IndexPutTemplate("global_logs-*").BodyString(mappingTemplate).Do()
	if err != nil {
		log.Errorf("Could not add index template; %s", err.Error())
		os.Exit(1)
	} else {
		log.Info("Index template added.")
	}
}

func configurateBulkProcess() {

	batch := 1000
	interval := 1 * time.Minute

	instance++

	processor, err := client.BulkProcessor().Before(beforeCommit).After(afterCommit).
		Name(fmt.Sprintf("global-logs-worker-%d", instance)).
		//Workers(concurrent).
		BulkActions(batch). // commit if # requests >= batch
		//BulkSize(maxsize).       // commit if size of requests >= maxsize
		FlushInterval(interval). // commit interval
		Do()
	if err != nil {
		log.Errorf("Elastic: bulc: %s", err.Error())
		return
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		select {
		case <-stop:
			// stop old
			if processor != nil {
				processor.Flush()
				processor.Stop()
				processor = nil
			}
			if client != nil {
				client.Flush()
				client.Stop()
				client = nil
			}
			log.Info("Elastic routine exit!")
			return
		}
	}()
}

func beforeCommit(executionID int64, requests []elastic.BulkableRequest) {
	wg.Add(1)
	log.WithFields(slf.Fields{
		"commitID:":        executionID,
		"len of requests:": len(requests),
	}).Debug("preparing commit")
}

func afterCommit(executionID int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	defer wg.Done() //wait for each commit. when all after commit is done, then we close es processor

	log.WithFields(slf.Fields{
		"finish commitID: ":  executionID,
		" len of requests: ": len(requests),
	}).Debug("commit done")

	//zero values by default
	var success int64
	var fail int64
	var skip int64

	shouldstop := false
	for idx, item := range response.Items {
		for _, resp := range item {
			if resp.Error == nil {
				success = success + 1
				continue //fast path, no error
			} else {
				fail = fail + 1
			}
			switch resp.Status {
			case TOO_MANY_REQUESTS:
				// alarm?
				time.Sleep(time.Duration(rand.Intn(50)+1) * time.Millisecond)
				fallthrough
			case SERVICE_UNAVAILABLE, INTERNAL_SERVER_ERROR:
				shouldstop = true //when encounters these errors, we thought stop consuming and wait for service
				/*r := requests[idx]
				switch {
				case sigexit: //already marked exit, no more push to processor again
					Backup(r)
				default:
					go func() {
						if !processor.AddTimeout(r, 100*time.Second) { //wait for 100 second to push, if not, backup
							Backup(r)
						}
					}()
				}*/
				log.Error(resp.Error.Reason)
			default:
				skip = skip + 1
				log.Warnf("Elasticsearch: insert failed:", requests[idx].String(), resp.Status, resp.Error.Reason)
				continue //skip
			}
		}
	}

	if err != nil || shouldstop {
		stopping = true
	} else {
		stopping = false
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
