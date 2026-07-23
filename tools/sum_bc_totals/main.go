// sum_bc_totals: computes SUM(tx_count), SUM(log_count), SUM(itx_count), SUM(contract_count)
// from block_completions for a given block range, scanning chunk by chunk in parallel.
// Use --from/--to to fix the snapshot block so results don't include new realtime blocks.
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
	ks       := flag.String("keyspace", "eth", "keyspace")
	from     := flag.Int64("from", 0, "start block (inclusive)")
	to       := flag.Int64("to", 0, "end block (inclusive, required)")
	workers  := flag.Int("workers", 16, "parallel workers")
	timeout  := flag.Duration("timeout", 120*time.Second, "query timeout per chunk")
	retries  := flag.Int("retries", 5, "retries per chunk")
	lanes    := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize  := flag.Int64("era-size", defaultEraSize, "blocks per era")
	flag.Parse()

	if *pass == "" { log.Fatal("--pass required") }
	if *to == 0    { log.Fatal("--to required") }

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *ks
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = *timeout
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil { log.Fatalf("connect: %v", err) }
	defer session.Close()

	minEra := *from / *eraSize
	maxEra := *to / *eraSize
	var chunks []int
	for era := minEra; era <= maxEra; era++ {
		for lane := 0; lane < *lanes; lane++ {
			chunks = append(chunks, lane+*lanes*int(era))
		}
	}
	log.Printf("from=%d to=%d eras=%d-%d chunks=%d workers=%d",
		*from, *to, minEra, maxEra, len(chunks), *workers)

	q := `SELECT tx_count, log_count, itx_count, contract_count
	      FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?`

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks { chunkCh <- c }
	close(chunkCh)

	var (
		sumTx, sumLog, sumItx, sumCont, sumRows atomic.Int64
		errCount                                 atomic.Int64
		wg                                       sync.WaitGroup
	)
	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				var success bool
				delay := 500 * time.Millisecond
				for attempt := 0; attempt < *retries; attempt++ {
					if attempt > 0 { time.Sleep(delay); delay *= 2 }
					var (
						lTx, lLog, lItx, lCont, lRows int64
						tx, lg, itx, cont              int
					)
					iter := session.Query(q, chunk, *from, *to).Iter()
					for iter.Scan(&tx, &lg, &itx, &cont) {
						lTx += int64(tx); lLog += int64(lg)
						lItx += int64(itx); lCont += int64(cont)
						lRows++
					}
					if err := iter.Close(); err != nil {
						log.Printf("[retry %d/%d] chunk=%d: %v", attempt+1, *retries, chunk, err)
						continue
					}
					sumTx.Add(lTx); sumLog.Add(lLog)
					sumItx.Add(lItx); sumCont.Add(lCont)
					sumRows.Add(lRows)
					success = true
					break
				}
				if !success {
					errCount.Add(1)
					log.Printf("[ERROR] chunk=%d: all retries failed", chunk)
				}
			}
		}()
	}
	wg.Wait()

	fmt.Printf("\n=== BC TOTALS from=%d to=%d ===\n", *from, *to)
	fmt.Printf("BC rows (blocks):      %20d\n", sumRows.Load())
	fmt.Printf("SUM tx_count:          %20d\n", sumTx.Load())
	fmt.Printf("SUM log_count:         %20d\n", sumLog.Load())
	fmt.Printf("SUM itx_count:         %20d\n", sumItx.Load())
	fmt.Printf("SUM contract_count:    %20d\n", sumCont.Load())
	fmt.Printf("\nErrors: %d  Elapsed: %.1fs\n", errCount.Load(), time.Since(start).Seconds())
}
