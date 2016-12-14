package main

import (
	"os"
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

var log = slf.WithContext("elastic-monitoring")

var options = []func(*stomp.Conn) error{}

var uuid string

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
	URL string
	//	TemplateName string
	Remapping bool
	//ID        string
	//Name      string
	Index string
}

// GlobalConf is a struct with global options,
// like server address and queue format, etc.
type ConfigFile struct {
	DirWithUUID   string
	Name          string
	Subscriptions []Subs
	ElasticServer Elastic `json:"Elastic"`
}

var globalOpt = ConfigFile{
	Name:          "default-name",
	DirWithUUID:   ".stomp_elastic/",
	Subscriptions: []Subs{},
}

//-------------------------------------------------------------------

/*func prepareElasticIndexTemplate() {

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
}*/

func parseMessage(msg []byte) (*MessageParseResult, error) {

	// unmarshal check if msg has nessesary fields
	importantFields, err := checkMsgForValid(msg)
	if err != nil {
		return nil, err
	}

	lenUTCFormat := len((*importantFields).Utc)
	var t time.Time

	switch {
	case lenUTCFormat == lenRfc3339:
		{
			//TODO: check Z/+/- usage for negative offset timezones!
			t, err = time.Parse(time.RFC3339, (*importantFields).Utc)
		}
	case lenUTCFormat < lenRfc3339:
		{
			if lenUTCFormat == lenRfc3339WithoutZ {
				//UTC without explicit zone marker
				t, err = time.Parse("2006-01-02T15:04:05", (*importantFields).Utc)
			}
			if lenUTCFormat == lenRfc3339WithZ {
				//UTC with explicit Z marker
				t, err = time.Parse("2006-01-02T15:04:05Z", (*importantFields).Utc)
			}
		}
	case lenUTCFormat > lenRfc3339:
		{
			//strange non-standard format, why?
			if lenUTCFormat == lenRfc3339WithTimeZone {
				log.Warnf("parseMessage: non-standard time format %s", (*importantFields).Utc)
				t, err = time.Parse("2006-01-02T15:04:05 -0700 MST", (*importantFields).Utc)
			}
		}
	default:
		{
			log.Warnf("Unknown utc format: %s", (*importantFields).Utc)
			err = ErrUnknownUTCFormat
		}
	}

	if err != nil {
		return nil, err
	}

	formattedMsg := addProcessNameShort(msg, (*importantFields).ProcessName)
	deleteNewlineSym(&formattedMsg)

	return &MessageParseResult{
		IndexPrefix:      importantFields.ElasticIndexPrefix,
		IndexDatePostfix: t.In(time.UTC).Format("2006-01-02"),
		FormattedMessage: formattedMsg,
	}, nil
}

func Connect(address string, login string, passcode string) (*stomp.Conn, error) {

	options = []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(login, passcode),
		stomp.ConnOpt.Host(address),
		stomp.ConnOpt.Header("wormmq.link.peer", uuid),
		stomp.ConnOpt.Header("wormmq.link.peer_name", globalOpt.Name),
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
		"queue": subNode.Queue,
		//"elasticIndex": subNode.Index,
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
			log.Warnf("Got empty message; ignore, err:%s", err.Error())
			continue
		}

		log.Debugf("!!! msg r=%s", msg.Body)

		var parsedMsg *MessageParseResult
		//check if message has necessary fields; adding fields
		if parsedMsg, err = parseMessage(msg.Body); err != nil {
			log.Debugf("msg with err=%s", msg.Body)
			continue
		}

		var indexName string
		if parsedMsg.IndexPrefix != "" {
			indexName = parsedMsg.IndexPrefix
		} else if subNode.Index != "" {
			indexName = subNode.Index
		} else {
			indexName = globalOpt.ElasticServer.Index
		}

		indexName += "-" + parsedMsg.IndexDatePostfix

		r := elastic.NewBulkIndexRequest().
			Index(indexName).
			Type(subNode.TypeName).
			Doc(parsedMsg.FormattedMessage)

		if r == nil {
			log.Error("nil")
			os.Exit(1)
		}

		//processor.Add(r)
		b.p.Add(r)

	}
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

func configurateBulkProcess() *Bulker {
	//b := &Bulker{c: client, index: globalOpt.ElasticServer.Index}
	b := &Bulker{c: client}
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

func main() {

	var err error
	config.ReadGlobalConfig(&globalOpt, "go-elastic-monitoring options")
	uuid = config.GetUUID(globalOpt.DirWithUUID)

	log.Infof("BuildDate=%s\n", BuildDate)
	log.Infof("GitCommit=%s\n", GitCommit)
	log.Infof("GitBranch=%s\n", GitBranch)
	log.Infof("GitState=%s\n", GitState)
	log.Infof("GitSummary=%s\n", GitSummary)
	log.Infof("VERSION=%s\n", Version)

	client, err = elastic.NewClient(elastic.SetURL(globalOpt.ElasticServer.URL))
	if err != nil {
		log.Error("elasicsearch: could not create client")
		os.Exit(1)
	}

	log.Info("Starting working...")

	/*if globalOpt.ElasticServer.Remapping {
		prepareElasticIndexTemplate()
	}*/

	b := configurateBulkProcess()
	defer b.Close()

	go recvMessages(b)
	<-stop
}
