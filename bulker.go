// this code was taken from an example of using elastic's BulkProcessor with Elasticsearch.

package main

import (
	_ "github.com/KristinaEtc/slflog"

	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

// printStats retrieves statistics from the Bulker and prints them.
func printStats(b *Bulker) {
	stats := b.Stats()
	var buf bytes.Buffer
	for i, w := range stats.Workers {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(fmt.Sprintf("%d=[%04d]", i, w.Queued))
	}

	log.Infof("%s | calls B=%04d,A=%04d,S=%04d,F=%04d | stats I=%05d,S=%05d,F=%05d | %v\n",
		time.Now().Format("15:04:05"),
		b.beforeCalls,
		b.afterCalls,
		b.successCalls,
		b.failureCalls,
		stats.Indexed,
		stats.Succeeded,
		stats.Failed,
		buf.String())
}

// -- Bulker --

// Bulker is an example for a process that needs to push data into
// Elasticsearch via BulkProcessor.
type Bulker struct {
	c       *elastic.Client
	p       *elastic.BulkProcessor
	workers int
	//index   string

	beforeCalls  int64         // # of calls into before callback
	afterCalls   int64         // # of calls into after callback
	failureCalls int64         // # of successful calls into after callback
	successCalls int64         // # of successful calls into after callback
	seq          int64         // sequential id
	stopC        chan struct{} // stop channel for the indexer

	throttleMu sync.Mutex // guards the following block
	throttle   bool       // throttle (or stop) sending data into bulk processor?
}

// Run starts the Bulker.
func (b *Bulker) Run() error {
	// Recreate Elasticsearch index
	/*if err := b.ensureIndex(); err != nil {
		return err
	}*/

	// Start bulk processor
	p, err := b.c.BulkProcessor().
		//Workers(b.workers).              // # of workers
		BulkActions(1000).               // # of queued requests before committed
		BulkSize(4096).                  // # of bytes in requests before committed
		FlushInterval(10 * time.Second). // autocommit every 10 seconds
		Stats(true).                     // gather statistics
		Before(b.before).                // call "before" before every commit
		After(b.after).                  // call "after" after every commit
		Do()
	if err != nil {
		return err
	}

	b.p = p

	// Start indexer that pushes data into bulk processor
	b.stopC = make(chan struct{})
	//go b.indexer()

	return nil
}

// Close the bulker.
func (b *Bulker) Close() error {
	b.stopC <- struct{}{}
	<-b.stopC
	close(b.stopC)
	return nil
}

/*
// indexer is a goroutine that periodically pushes data into
// bulk processor unless being "throttled" or "stopped".
// i don't use it
func (b *Bulker) indexer() {
	var stop bool

	for !stop {
		select {
		case <-b.stopC:
			stop = true

		default:
			b.throttleMu.Lock()
			throttled := b.throttle
			b.throttleMu.Unlock()

			if !throttled {
				// Sample data structure
				doc := struct {
					Seq int64 `json:"seq"`
				}{
					Seq: atomic.AddInt64(&b.seq, 1),
				}
				// Add bulk request.
				// Notice that we need to set Index and Type here!
				r := elastic.NewBulkIndexRequest().Index(b.index).Type("doc").Doc(doc)
				b.p.Add(r)
			}
			// Sleep for a short time.
			time.Sleep(time.Duration(rand.Intn(7)) * time.Millisecond)
		}
	}

	b.stopC <- struct{}{} // ack stopping
}
*/

// before is invoked from bulk processor before every commit.
func (b *Bulker) before(id int64, requests []elastic.BulkableRequest) {
	atomic.AddInt64(&b.beforeCalls, 1)
}

// after is invoked by bulk processor after every commit.
// The err variable indicates success or failure.
func (b *Bulker) after(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	atomic.AddInt64(&b.afterCalls, 1)

	b.throttleMu.Lock()
	if err != nil {
		atomic.AddInt64(&b.failureCalls, 1)
		b.throttle = true // bulk processor in trouble
	} else {
		atomic.AddInt64(&b.successCalls, 1)
		b.throttle = false // bulk processor ok
	}
	b.throttleMu.Unlock()
}

// Stats returns statistics from bulk processor.
func (b *Bulker) Stats() elastic.BulkProcessorStats {
	return b.p.Stats()
}

/*
// ensureIndex creates the index in Elasticsearch.
// It will be dropped if it already exists.
func (b *Bulker) ensureIndex() error {
	if b.index == "" {
		return errors.New("no index name")
	}
	exists, err := b.c.IndexExists(b.index).Do()
	if err != nil {
		return err
	}
	if exists {
		_, err = b.c.DeleteIndex(b.index).Do()
		if err != nil {
			return err
		}
	}
	_, err = b.c.CreateIndex(b.index).Do()
	if err != nil {
		return err
	}
	return nil
}*/
