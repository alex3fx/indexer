// count_creates_by_block: counts rows in contracts_by_address_v2 filtered
// by block_number range using token-range segments + ALLOW FILTERING.
// Used to split total row count into pre-/post-Byzantium buckets for
// comparison against Dune ethereum.traces counts.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "user")
	pass     := flag.String("pass", "", "password")
	keyspace := flag.String("keyspace", "eth", "keyspace")
	segments := flag.Int("segments", 256, "token-range segments")
	workers  := flag.Int("workers", 16, "parallel workers")
	timeout  := flag.Duration("timeout", 180*time.Second, "Scylla query timeout per segment")
	retries  := flag.Int("retries", 8, "retry attempts per segment")
	blockLo  := flag.Int64("block-lo", 0, "inclusive lower bound for block_number")
	blockHi  := flag.Int64("block-hi", math.MaxInt32, "inclusive upper bound for block_number")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
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

	log.Printf("counting rows with block_number in [%d, %d]", *blockLo, *blockHi)

	tokenMin := int64(math.MinInt64)
	tokenMax := int64(math.MaxInt64)
	step := uint64(tokenMax-tokenMin) / uint64(*segments)

	type seg struct{ lo, hi int64 }
	segCh := make(chan seg, *segments)
	for i := 0; i < *segments; i++ {
		lo := tokenMin + int64(uint64(i)*step)
		var hi int64
		if i == *segments-1 {
			hi = tokenMax
		} else {
			hi = tokenMin + int64(uint64(i+1)*step) - 1
		}
		segCh <- seg{lo, hi}
	}
	close(segCh)

	var totalRows atomic.Int64
	var segsProcessed atomic.Int64
	var segErrors atomic.Int64

	start := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := segsProcessed.Load()
				r := totalRows.Load()
				log.Printf("[progress] segs=%d/%d rows_so_far=%d elapsed=%.1fs",
					p, *segments, r, time.Since(start).Seconds())
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range segCh {
				var count int64
				err := retryQuery(session, s.lo, s.hi, *blockLo, *blockHi, *retries, &count)
				if err != nil {
					log.Printf("[ERROR] seg [%d,%d]: %v", s.lo, s.hi, err)
					segErrors.Add(1)
				} else {
					totalRows.Add(count)
				}
				segsProcessed.Add(1)
			}
		}()
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	total := totalRows.Load()
	errors := segErrors.Load()

	fmt.Printf("\n=== RESULT ===\n")
	fmt.Printf("table:             contracts_by_address_v2\n")
	fmt.Printf("block_number:      [%d, %d]\n", *blockLo, *blockHi)
	fmt.Printf("segments:          %d\n", *segments)
	fmt.Printf("segment errors:    %d\n", errors)
	fmt.Printf("total rows:        %d\n", total)
	fmt.Printf("elapsed:           %.1fs\n", elapsed.Seconds())
	if errors > 0 {
		fmt.Printf("\nWARNING: %d segments failed — total is a lower bound\n", errors)
	}
}

func retryQuery(session *gocql.Session, lo, hi, blockLo, blockHi int64, maxRetries int, count *int64) error {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		err := session.Query(
			`SELECT COUNT(*) FROM contracts_by_address_v2
			 WHERE token(address) >= ? AND token(address) <= ?
			 AND block_number >= ? AND block_number <= ?
			 ALLOW FILTERING`,
			lo, hi, blockLo, blockHi,
		).Scan(count)
		if err == nil {
			return nil
		}
		log.Printf("[retry %d/%d] seg [%d,%d]: %v", attempt+1, maxRetries, lo, hi, err)
	}
	return fmt.Errorf("all %d retries failed for seg [%d,%d]", maxRetries, lo, hi)
}
