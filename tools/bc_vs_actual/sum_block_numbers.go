// sum_block_numbers: computes SUM(block_number) and COUNT(*) from block_completions
// and compares with the expected arithmetic progression sum for the given range.
// If SUM matches the arithmetic series, all blocks are present in BC.
// Discrepancy reveals missing or duplicated block entries.
//
// +build ignore

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
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "port")
	user     := flag.String("user", "cassandra", "user")
	pass     := flag.String("pass", "", "password")
	keyspace := flag.String("keyspace", "eth", "keyspace")
	from     := flag.Int64("from", 0, "start block (inclusive)")
	to       := flag.Int64("to", 0, "end block (inclusive)")
	workers  := flag.Int("workers", 12, "parallel workers")
	timeout  := flag.Duration("timeout", 120*time.Second, "query timeout")
	retries  := flag.Int("retries", 5, "retries per chunk")
	lanes    := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize  := flag.Int64("era-size", defaultEraSize, "blocks per era")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}
	if *to == 0 {
		log.Fatal("--to required")
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
	maxEra := *to / *eraSize
	var chunks []int
	for era := minEra; era <= maxEra; era++ {
		for lane := 0; lane < *lanes; lane++ {
			chunks = append(chunks, lane+*lanes*int(era))
		}
	}

	n := *to - *from + 1
	expectedSum := n / 2 * (*from + *to)
	if n%2 == 1 {
		expectedSum = (n/2)*(*from+*to) + (*from+*to)/2
	}
	// More precise: sum = n*(first+last)/2
	expectedSum = n * (*from + *to) / 2

	log.Printf("from=%d to=%d eras=%d-%d chunks=%d workers=%d", *from, *to, minEra, maxEra, len(chunks), *workers)
	log.Printf("expected blocks: %d", n)
	log.Printf("expected SUM(block_number): %d", expectedSum)

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var totalSum, totalCount atomic.Int64
	var errCount atomic.Int64
	var done atomic.Int64

	start := time.Now()
	ticker := time.NewTicker(15 * time.Second)
	stopTick := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Printf("progress: chunks=%d/%d count=%d sum=%d errors=%d elapsed=%.0fs",
					done.Load(), len(chunks), totalCount.Load(), totalSum.Load(), errCount.Load(), time.Since(start).Seconds())
			case <-stopTick:
				ticker.Stop()
				return
			}
		}
	}()

	// Query BC for block_number column (not tx_count) to get SUM and COUNT
	bcQuery := `SELECT block_number FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?`

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				var localSum, localCount int64
				var delay time.Duration = 500 * time.Millisecond
				var success bool
				for attempt := 0; attempt < *retries; attempt++ {
					if attempt > 0 {
						time.Sleep(delay)
						if delay < 32*time.Second {
							delay *= 2
						}
					}
					iter := session.Query(bcQuery, chunk, *from, *to).Iter()
					localSum = 0
					localCount = 0
					var bn int64
					for iter.Scan(&bn) {
						localSum += bn
						localCount++
					}
					if err := iter.Close(); err != nil {
						log.Printf("[retry %d/%d] chunk=%d: %v", attempt+1, *retries, chunk, err)
						continue
					}
					success = true
					break
				}
				if !success {
					errCount.Add(1)
					log.Printf("[ERROR] chunk=%d: all retries failed", chunk)
				} else {
					totalSum.Add(localSum)
					totalCount.Add(localCount)
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	close(stopTick)

	actualSum := totalSum.Load()
	actualCount := totalCount.Load()
	sumDiff := actualSum - expectedSum
	countDiff := actualCount - n

	fmt.Printf("\n=== RESULTS from=%d to=%d ===\n", *from, *to)
	fmt.Printf("Expected blocks:       %d\n", n)
	fmt.Printf("BC row count:          %d\n", actualCount)
	fmt.Printf("Count diff:            %+d\n", countDiff)
	fmt.Printf("\nExpected SUM:          %d\n", expectedSum)
	fmt.Printf("Actual BC SUM:         %d\n", actualSum)
	fmt.Printf("SUM diff:              %+d\n", sumDiff)
	fmt.Printf("\nErrors:                %d\n", errCount.Load())
	fmt.Printf("Elapsed:               %.1fs\n", time.Since(start).Seconds())

	if countDiff == 0 && sumDiff == 0 {
		fmt.Println("\nCONCLUSION: BC is COMPLETE — all blocks present, no duplicates")
	} else if countDiff < 0 {
		fmt.Printf("\nCONCLUSION: BC is INCOMPLETE — %d blocks MISSING from block_completions\n", -countDiff)
		fmt.Printf("  Average missing block number ≈ %d\n", sumDiff/countDiff)
	} else if countDiff > 0 {
		fmt.Printf("\nCONCLUSION: BC has DUPLICATES — %d extra rows\n", countDiff)
	}
}
