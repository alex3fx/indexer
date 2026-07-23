// count_precompile_itx: counts rows in eth.internal_transactions where
// to_address is a classical precompile (0x01-0x0a), for blocks 0..--to.
// These are direct EOA transactions to precompile addresses traced by reth
// but excluded from dune_adj_old formula.
// Scans chunk by chunk in parallel (same approach as sum_bc_totals).
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

var precompileAddrs = []string{
	"0x0000000000000000000000000000000000000001",
	"0x0000000000000000000000000000000000000002",
	"0x0000000000000000000000000000000000000003",
	"0x0000000000000000000000000000000000000004",
	"0x0000000000000000000000000000000000000005",
	"0x0000000000000000000000000000000000000006",
	"0x0000000000000000000000000000000000000007",
	"0x0000000000000000000000000000000000000008",
	"0x0000000000000000000000000000000000000009",
	"0x000000000000000000000000000000000000000a",
}

func main() {
	host    := flag.String("host", "127.0.0.1", "Scylla host")
	port    := flag.Int("port", 9042, "port")
	user    := flag.String("user", "cassandra", "user")
	pass    := flag.String("pass", "", "password")
	ks      := flag.String("keyspace", "eth", "keyspace")
	from    := flag.Int64("from", 0, "start block (inclusive)")
	to      := flag.Int64("to", 0, "end block (inclusive, required)")
	workers := flag.Int("workers", 16, "parallel workers")
	timeout := flag.Duration("timeout", 120*time.Second, "query timeout per chunk")
	retries := flag.Int("retries", 5, "retries per chunk")
	lanes   := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize := flag.Int64("era-size", defaultEraSize, "blocks per era")
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
	cluster.Keyspace = *ks
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
	log.Printf("from=%d to=%d eras=%d-%d chunks=%d workers=%d",
		*from, *to, minEra, maxEra, len(chunks), *workers)

	// Build IN list placeholders for precompile addresses
	inList := "?"
	for i := 1; i < len(precompileAddrs); i++ {
		inList += ",?"
	}
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM internal_transactions WHERE chunk=? AND block_number>=? AND block_number<=? AND to_address IN (%s) ALLOW FILTERING",
		inList,
	)

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var (
		total    atomic.Int64
		errCount atomic.Int64
		wg       sync.WaitGroup
	)
	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				var cnt int64
				var lastErr error
				for attempt := 0; attempt <= *retries; attempt++ {
					if attempt > 0 {
						time.Sleep(time.Duration(attempt) * 2 * time.Second)
					}
					args := []interface{}{chunk, *from, *to}
					for _, addr := range precompileAddrs {
						args = append(args, addr)
					}
					if err := session.Query(q, args...).Scan(&cnt); err != nil {
						lastErr = err
						continue
					}
					lastErr = nil
					break
				}
				if lastErr != nil {
					log.Printf("ERROR chunk=%d: %v", chunk, lastErr)
					errCount.Add(1)
					continue
				}
				total.Add(cnt)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== COUNT PRECOMPILE ITX from=%d to=%d ===\n", *from, *to)
	fmt.Printf("Precompile calls (0x01-0x0a) in internal_transactions: %d\n", total.Load())
	fmt.Printf("Errors: %d  Elapsed: %.1fs\n", errCount.Load(), elapsed.Seconds())
}
