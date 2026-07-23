// count_distinct_per_million: counts DISTINCT addresses in contracts_by_address_v2
// grouped by million-block ranges (m=0 → blocks 0–999999, m=1 → 1000000–1999999, …).
//
// Uses the same 256-segment token-range parallel scan as count_distinct_historical.
// Within each segment, rows arrive sorted by token(address), then block_number, so
// distinct-address counting is O(1) extra memory per segment.
//
// For comparison with Dune: COUNT(DISTINCT address) per million WHERE
//   type='create' AND success=true AND tx_success=true AND block_number <= blockHi
//
// Usage:
//   ./count_distinct_per_million --pass=cassandra --block-hi=25422000
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

const maxMillions = 30 // up to M29 (block 29,999,999)

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

	// Per-million counters (global, atomic)
	var millionCounts [maxMillions]atomic.Int64
	var segsProcessed atomic.Int64
	var segErrors atomic.Int64
	var totalRows atomic.Int64

	start := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := segsProcessed.Load()
				elapsed := time.Since(start).Seconds()
				var eta string
				if p > 0 {
					remaining := int64(*segments) - p
					etaSec := elapsed / float64(p) * float64(remaining)
					eta = fmt.Sprintf(" eta=%.0fs", etaSec)
				}
				log.Printf("[progress] segs=%d/%d rows=%d elapsed=%.1fs%s",
					p, *segments, totalRows.Load(), elapsed, eta)
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range segCh {
				counts, rows, err := scanSegment(session, s.lo, s.hi, *blockHi, *pageSize, *retries)
				if err != nil {
					log.Printf("[ERROR] seg [%d,%d]: %v", s.lo, s.hi, err)
					segErrors.Add(1)
				} else {
					mu.Lock()
					for m, c := range counts {
						millionCounts[m].Add(c)
					}
					mu.Unlock()
					totalRows.Add(rows)
				}
				segsProcessed.Add(1)
			}
		}()
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	errors := segErrors.Load()

	maxM := int(*blockHi/1000000) + 1
	if maxM > maxMillions {
		maxM = maxMillions
	}

	fmt.Printf("\n=== RESULT: DISTINCT addresses per million blocks ===\n")
	fmt.Printf("table:    contracts_by_address_v2\n")
	fmt.Printf("block_hi: %d\n", *blockHi)
	fmt.Printf("segments: %d  errors: %d  rows: %d  elapsed: %s\n\n",
		*segments, errors, totalRows.Load(), elapsed.Round(time.Second))
	fmt.Printf("%-4s  %14s\n", "M", "distinct_addrs")
	fmt.Printf("%-4s  %14s\n", "---", "--------------")
	var total int64
	for m := 0; m < maxM; m++ {
		c := millionCounts[m].Load()
		if c > 0 {
			fmt.Printf("%-4d  %14d\n", m, c)
			total += c
		}
	}
	fmt.Printf("%-4s  %14d\n", "TOT", total)

	if errors > 0 {
		fmt.Printf("\nWARNING: %d segments failed — counts are lower bounds\n", errors)
	}
}

// scanSegment returns per-million distinct address counts for one token-range segment.
// Relies on rows being delivered sorted by token(address) ASC, block_number ASC within.
func scanSegment(session *gocql.Session, lo, hi, blockHi int64, pageSize, maxRetries int) ([maxMillions]int64, int64, error) {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		counts, rows, err := tryScan(session, lo, hi, blockHi, pageSize)
		if err == nil {
			return counts, rows, nil
		}
		log.Printf("[retry %d/%d] seg [%d,%d]: %v", attempt+1, maxRetries, lo, hi, err)
	}
	return [maxMillions]int64{}, 0, fmt.Errorf("all %d retries failed for seg [%d,%d]", maxRetries, lo, hi)
}

func tryScan(session *gocql.Session, lo, hi, blockHi int64, pageSize int) ([maxMillions]int64, int64, error) {
	iter := session.Query(
		`SELECT address, block_number FROM contracts_by_address_v2
		 WHERE token(address) >= ? AND token(address) <= ?
		   AND block_number <= ?
		 ALLOW FILTERING`,
		lo, hi, blockHi,
	).PageSize(pageSize).Iter()

	var (
		counts      [maxMillions]int64
		addr, prev  string
		bn          int64
		rows        int64
		// Which millions the current address has appeared in (within this segment).
		// Max realistic value: an address deployed in several different millions.
		// Use a small fixed array; most addresses appear in exactly 1 million.
		curMillions [maxMillions]bool
	)

	flush := func() {
		if prev == "" {
			return
		}
		for m, seen := range curMillions {
			if seen {
				counts[m]++
				curMillions[m] = false
			}
		}
	}

	for iter.Scan(&addr, &bn) {
		rows++
		m := int(bn / 1000000)
		if m >= maxMillions {
			continue
		}
		if addr != prev {
			flush()
			prev = addr
		}
		curMillions[m] = true
	}
	flush() // final address group

	if err := iter.Close(); err != nil {
		return [maxMillions]int64{}, 0, err
	}
	return counts, rows, nil
}
