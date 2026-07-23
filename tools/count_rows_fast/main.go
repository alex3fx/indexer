// count_rows_fast: counts total rows in contracts_by_address_v2 using
// SELECT COUNT(*) per small token-range segment with a bounded worker pool.
// Avoids SELECT DISTINCT paging issues (which hang at ~51M due to 104 SSTables).
//
// Gives total rows (deployment events). Unique addresses ≈ total - ~330k re-deploys.
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
	timeout  := flag.Duration("timeout", 120*time.Second, "Scylla query timeout")
	retries  := flag.Int("retries", 8, "retry attempts per segment")
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

	// Progress ticker
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
				err := retryQuery(session, s.lo, s.hi, *retries, &count)
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
	fmt.Printf("segments:          %d\n", *segments)
	fmt.Printf("segment errors:    %d\n", errors)
	fmt.Printf("total rows:        %d\n", total)
	fmt.Printf("elapsed:           %.1fs\n", elapsed.Seconds())
	if errors > 0 {
		fmt.Printf("\nWARNING: %d segments failed — total is a lower bound\n", errors)
	} else {
		fmt.Printf("unique addrs est:  ~%d (total - ~330k re-deploys)\n", total-330000)
	}
}

func retryQuery(session *gocql.Session, lo, hi int64, maxRetries int, count *int64) error {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		err := session.Query(
			`SELECT COUNT(*) FROM contracts_by_address_v2 WHERE token(address) >= ? AND token(address) <= ?`,
			lo, hi,
		).Scan(count)
		if err == nil {
			return nil
		}
		log.Printf("[retry %d/%d] seg [%d,%d]: %v", attempt+1, maxRetries, lo, hi, err)
	}
	return fmt.Errorf("all %d retries failed for seg [%d,%d]", maxRetries, lo, hi)
}
