// sum_block_completions: sums tx_count, log_count, itx_count from block_completions
// up to a fixed checkpoint block. Used for cross-validation against Dune Analytics.
package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

const (
	defaultLanes   = 24
	defaultEraSize = 12000
)

func main() {
	host       := flag.String("host", "127.0.0.1", "Scylla host")
	port       := flag.Int("port", 9042, "Scylla port")
	user       := flag.String("user", "cassandra", "user")
	pass       := flag.String("pass", "", "password")
	keyspace   := flag.String("keyspace", "eth", "keyspace")
	from       := flag.Int64("from", 0, "start block number (inclusive, 0 = genesis)")
	checkpoint := flag.Int64("checkpoint", 0, "end block number (inclusive upper bound)")
	workers    := flag.Int("workers", 8, "parallel workers")
	timeout    := flag.Duration("timeout", 120*time.Second, "Scylla query timeout")
	retries    := flag.Int("retries", 8, "retry attempts per chunk")
	lanes      := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize    := flag.Int64("era-size", defaultEraSize, "blocks per era")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}
	if *checkpoint == 0 {
		log.Fatal("--checkpoint required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = *timeout
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	minEra := *from / *eraSize
	maxEra := *checkpoint / *eraSize
	var chunks []int
	for era := minEra; era <= maxEra; era++ {
		for lane := 0; lane < *lanes; lane++ {
			chunks = append(chunks, lane+*lanes*int(era))
		}
	}
	log.Printf("from=%d  checkpoint=%d  eras=%d-%d  chunks=%d  workers=%d  timeout=%s  retries=%d",
		*from, *checkpoint, minEra, maxEra, len(chunks), *workers, *timeout, *retries)

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var txTotal, logTotal, itxTotal atomic.Int64
	var errCount atomic.Int64
	var done atomic.Int64

	start := time.Now()

	logTicker := time.NewTicker(30 * time.Second)
	stopLog := make(chan struct{})
	go func() {
		for {
			select {
			case <-logTicker.C:
				log.Printf("chunks=%d/%d  tx=%d  log=%d  itx=%d  errors=%d  elapsed=%.0fs",
					done.Load(), len(chunks), txTotal.Load(), logTotal.Load(), itxTotal.Load(), errCount.Load(), time.Since(start).Seconds())
			case <-stopLog:
				logTicker.Stop()
				return
			}
		}
	}()

	query := `SELECT SUM(tx_count), SUM(log_count), SUM(itx_count) FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?`

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				var tx, lg, itx int64
				err := retryChunk(session, query, chunk, *from, *checkpoint, *retries, &tx, &lg, &itx)
				if err != nil {
					errCount.Add(1)
					log.Printf("[ERROR] chunk=%d: %v", chunk, err)
				} else {
					txTotal.Add(tx)
					logTotal.Add(lg)
					itxTotal.Add(itx)
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	close(stopLog)

	elapsed := time.Since(start)
	fmt.Printf("\n=== RESULTS (checkpoint=%d) ===\n", *checkpoint)
	fmt.Printf("tx_count sum:  %d\n", txTotal.Load())
	fmt.Printf("log_count sum: %d\n", logTotal.Load())
	fmt.Printf("itx_count sum: %d\n", itxTotal.Load())
	fmt.Printf("errors:        %d\n", errCount.Load())
	fmt.Printf("elapsed:       %.1fs\n", elapsed.Seconds())
}

func retryChunk(session *gocql.Session, query string, chunk int, from, checkpoint int64, maxRetries int, tx, lg, itx *int64) error {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		err := session.Query(query, chunk, from, checkpoint).Scan(tx, lg, itx)
		if err == nil {
			return nil
		}
		log.Printf("[retry %d/%d] chunk=%d: %v", attempt+1, maxRetries, chunk, err)
	}
	return fmt.Errorf("all %d retries failed (chunk=%d)", maxRetries, chunk)
}
