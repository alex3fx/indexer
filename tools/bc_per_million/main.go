// bc_per_million: sums itx_count from eth.block_completions, grouped by 1M-block ranges.
// Used to compare BC sums against Dune per-million trace counts.
// Output: CSV with million_start,bc_itx_count columns.
package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

const (
	defaultLanes   = 24
	defaultEraSize = 12000
)

func main() {
	host    := flag.String("host", "127.0.0.1", "Scylla host")
	port    := flag.Int("port", 9042, "port")
	user    := flag.String("user", "cassandra", "user")
	pass    := flag.String("pass", "", "password")
	ks      := flag.String("keyspace", "eth", "keyspace")
	from    := flag.Int64("from", 0, "start block (inclusive)")
	to      := flag.Int64("to", 0, "end block (inclusive, required)")
	workers := flag.Int("workers", 32, "parallel workers")
	timeout     := flag.Duration("timeout", 120*time.Second, "query timeout per chunk")
	retries     := flag.Int("retries", 5, "retries per chunk")
	lanes       := flag.Int64("lanes", defaultLanes, "chunk lanes")
	eraSize     := flag.Int64("era-size", defaultEraSize, "blocks per era")
	granularity := flag.Int64("granularity", 1_000_000, "bucket size in blocks")
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
	cluster.NumConns = 8
	session, err := cluster.CreateSession()
	if err != nil { log.Fatalf("connect: %v", err) }
	defer session.Close()

	minEra := *from / *eraSize
	maxEra := *to / *eraSize
	var chunks []int64
	for era := minEra; era <= maxEra; era++ {
		for lane := int64(0); lane < *lanes; lane++ {
			chunks = append(chunks, lane+*lanes*era)
		}
	}
	log.Printf("from=%d to=%d eras=%d-%d chunks=%d workers=%d",
		*from, *to, minEra, maxEra, len(chunks), *workers)

	q := `SELECT block_number, itx_count FROM block_completions WHERE chunk=?`

	// Per-granularity accumulator, protected by mutex
	numMillions := int((*to-*from)/(*granularity)) + 2
	itxByMillion := make([]int64, numMillions)
	var mu sync.Mutex

	chunkCh := make(chan int64, len(chunks))
	for _, c := range chunks { chunkCh <- c }
	close(chunkCh)

	var (
		errCount int64
		wg       sync.WaitGroup
	)
	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make(map[int]int64)
			for chunk := range chunkCh {
				var success bool
				delay := 300 * time.Millisecond
				for attempt := 0; attempt < *retries; attempt++ {
					if attempt > 0 { time.Sleep(delay); delay *= 2 }
					clear(local)
					var blk int64
					var itx int
					iter := session.Query(q, chunk).Iter()
					for iter.Scan(&blk, &itx) {
						if blk < *from || blk > *to { continue }
						m := int((blk - *from) / (*granularity))
						local[m] += int64(itx)
					}
					if err := iter.Close(); err != nil {
						log.Printf("[retry %d/%d] chunk=%d: %v", attempt+1, *retries, chunk, err)
						continue
					}
					success = true
					break
				}
				if !success {
					mu.Lock()
					errCount++
					mu.Unlock()
					log.Printf("[ERROR] chunk=%d: all retries failed", chunk)
					continue
				}
				mu.Lock()
				for m, v := range local { itxByMillion[m] += v }
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	fmt.Println("bucket_start,bc_itx")
	for i, v := range itxByMillion {
		mStart := *from + int64(i)*(*granularity)
		if mStart > *to { break }
		fmt.Printf("%d,%d\n", mStart, v)
	}
	log.Printf("Errors: %d  Elapsed: %.1fs", errCount, time.Since(start).Seconds())
}
