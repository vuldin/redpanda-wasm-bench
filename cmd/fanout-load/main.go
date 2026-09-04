// Command fanout-load simulates realistic high-consumer-fanout load on
// a single hot topic - many independent consumer groups all fetching
// the same output topic, the way a real deployment's downstream risk
// systems, position trackers, audit logs, and UI feeds would. It exists
// to answer a question this project's benchmarking had never actually
// tested (every prior run used exactly one consumer): does heavy fetch
// load on an output topic degrade the wasm-inbroker path's own latency
// more than it degrades the external-client path's, since only the
// in-broker path shares CPU/scheduling with the broker's own
// fetch-serving work (the transforms scheduling group's CPU shares
// versus fetch's).
//
// Each simulated consumer is its own goroutine with its own client and
// its own, uniquely-named consumer group - N independent groups, not N
// members of one group - so this reproduces "N systems each reading
// everything" fanout, not partition-sharing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokers  = flag.String("brokers", "localhost:9092", "comma-separated seed brokers")
		topic    = flag.String("topic", "fills", "topic to fan out consumers against")
		n        = flag.Int("n", 100, "number of independent consumer groups to simulate")
		duration = flag.Duration("duration", 0, "how long to run before exiting; 0 means run until killed")
		groupPfx = flag.String("group-prefix", "fanout", "prefix for each simulated consumer group's name")
	)
	flag.Parse()

	seeds := strings.Split(*brokers, ",")
	ctx := context.Background()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	var totalRecords atomic.Int64
	var wg sync.WaitGroup
	runID := time.Now().UnixNano()
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			group := fmt.Sprintf("%s-%d-%d", *groupPfx, runID, idx)
			client, err := kgo.NewClient(
				kgo.SeedBrokers(seeds...),
				kgo.ConsumeTopics(*topic),
				kgo.ConsumerGroup(group),
			)
			if err != nil {
				log.Printf("fanout-load: consumer %d: creating client: %v", idx, err)
				return
			}
			defer client.Close()
			for {
				fetches := client.PollFetches(ctx)
				if ctx.Err() != nil {
					return
				}
				fetches.EachRecord(func(*kgo.Record) {
					totalRecords.Add(1)
				})
			}
		}(i)
	}

	log.Printf("fanout-load: %d independent consumer groups fetching %s", *n, *topic)
	if *duration > 0 {
		<-ctx.Done()
	} else {
		// Report progress periodically so it's visible this is alive
		// and doing real work, since there's otherwise no output
		// until the process is killed.
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C
			log.Printf("fanout-load: %d records fetched across %d groups so far",
				totalRecords.Load(), *n)
		}
	}
	wg.Wait()
	log.Printf("fanout-load: done, %d total records fetched across %d groups",
		totalRecords.Load(), *n)
}
