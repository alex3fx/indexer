// count_distinct_historical: counts unique addresses in contracts_by_address_v2
// where block_number <= --block-hi. Uses ALLOW FILTERING on the clustering key.
// Same parallel token-range scan as count_distinct_addrs_v3.
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
	blockHi  := flag.Int64("block-hi", 25422000, "max block_number (inclusive)")
	segments := flag.Int("segments", 256, "token-range segments")
	workers  := flag.Int("workers", 16, "parallel workers")
	pageSize := flag.Int("page-size", 5000, "CQL page size per segment")
	timeout  := flag.Duration("timeout", 180*time.Second, "Scylla query timeout")
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

	var totalUnique atomic.Int64
	var segsProcessed atomic.Int64
	var segErrors atomic.Int64

	start := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := segsProcessed.Load()
				u := totalUnique.Load()
				elapsed := time.Since(start).Seconds()
				var eta string
				if p > 0 {
					remaining := int64(*segments) - p
					etaSec := elapsed / float64(p) * float64(remaining)
					eta = fmt.Sprintf(" eta=%.0fs", etaSec)
				}
				log.Printf("[progress] segs=%d/%d unique=%d elapsed=%.1fs%s", p, *segments, u, elapsed, eta)
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
				count, err := countSegment(session, s.lo, s.hi, *blockHi, *pageSize, *retries)
				if err != nil {
					log.Printf("[ERROR] seg [%d,%d]: %v", s.lo, s.hi, err)
					segErrors.Add(1)
				} else {
					totalUnique.Add(count)
				}
				segsProcessed.Add(1)
			}
		}()
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	total := totalUnique.Load()
	errors := segErrors.Load()

	fmt.Printf("\n=== RESULT ===\n")
	fmt.Printf("table:             contracts_by_address_v2\n")
	fmt.Printf("block_hi:          %d\n", *blockHi)
	fmt.Printf("segments:          %d\n", *segments)
	fmt.Printf("segment errors:    %d\n", errors)
	fmt.Printf("unique addresses:  %d\n", total)
	fmt.Printf("elapsed:           %.1fs\n", elapsed.Seconds())
	if errors > 0 {
		fmt.Printf("\nWARNING: %d segments failed — count is a lower bound\n", errors)
	}
}

func countSegment(session *gocql.Session, lo, hi, blockHi int64, pageSize, maxRetries int) (int64, error) {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		count, err := tryCount(session, lo, hi, blockHi, pageSize)
		if err == nil {
			return count, nil
		}
		log.Printf("[retry %d/%d] seg [%d,%d]: %v", attempt+1, maxRetries, lo, hi, err)
	}
	return 0, fmt.Errorf("all %d retries failed for seg [%d,%d]", maxRetries, lo, hi)
}

func tryCount(session *gocql.Session, lo, hi, blockHi int64, pageSize int) (int64, error) {
	// block_number is the clustering key; ALLOW FILTERING needed to filter on it
	// without a full token scan per partition.
	// Rows come back ordered by token(address), so unique-address count
	// is just the number of address transitions. O(1) extra memory.
	iter := session.Query(
		`SELECT address FROM contracts_by_address_v2
		 WHERE token(address) >= ? AND token(address) <= ?
		   AND block_number <= ?
		 ALLOW FILTERING`,
		lo, hi, blockHi,
	).PageSize(pageSize).Iter()

	var addr, prevAddr string
	var unique int64
	for iter.Scan(&addr) {
		if addr != prevAddr {
			unique++
			prevAddr = addr
		}
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	return unique, nil
}
