// count_distinct_addrs: counts UNIQUE (distinct) contract addresses in contracts_by_address_v2
// using SELECT DISTINCT address across token-range segments.
// Compares unique address count vs Etherscan's 101,821,894 to determine if the +937k row gap
// is purely multi-row records (re-deployments + failed attempts) or includes phantom-only addresses.
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
	segments := flag.Int("segments", 256, "token-range segments")
	pageSize := flag.Int("page-size", 2000, "CQL page size per segment")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 4
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	var total atomic.Int64
	var errors atomic.Int64

	n := *segments
	totalF := float64(uint64(math.MaxUint64)) + 1.0
	stepF := totalF / float64(n)

	start := time.Now()
	log.Printf("Starting DISTINCT address count: segments=%d page-size=%d", n, *pageSize)

	go func() {
		for {
			time.Sleep(30 * time.Second)
			t := total.Load()
			elapsed := time.Since(start)
			rate := float64(t) / elapsed.Seconds()
			log.Printf("[progress] unique_addresses=%d rate=%.0f addr/s elapsed=%s",
				t, rate, elapsed.Round(time.Second))
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		lo := int64(math.MinInt64 + int64(float64(i)*stepF))
		last := i == n-1
		var hi int64
		if last {
			hi = math.MaxInt64
		} else {
			hi = int64(math.MinInt64 + int64(float64(i+1)*stepF))
		}

		wg.Add(1)
		go func(lo, hi int64, last bool) {
			defer wg.Done()

			var q *gocql.Query
			if last {
				q = session.Query(
					`SELECT DISTINCT address FROM eth.contracts_by_address_v2 WHERE token(address) >= ?`,
					lo,
				)
			} else {
				q = session.Query(
					`SELECT DISTINCT address FROM eth.contracts_by_address_v2 WHERE token(address) >= ? AND token(address) < ?`,
					lo, hi,
				)
			}
			iter := q.PageSize(*pageSize).Iter()

			var addr string
			var cnt int64
			for iter.Scan(&addr) {
				cnt++
			}
			if err := iter.Close(); err != nil {
				errors.Add(1)
				log.Printf("[err] segment lo=%d: %v", lo, err)
			}
			total.Add(cnt)
		}(lo, hi, last)
	}

	wg.Wait()

	elapsed := time.Since(start).Round(time.Second)
	t := total.Load()
	e := errors.Load()

	fmt.Println("=== RESULT ===")
	fmt.Printf("unique addresses:    %d\n", t)
	fmt.Printf("etherscan reference: 101,821,894\n")
	diff := int64(t) - 101821894
	if diff >= 0 {
		fmt.Printf("difference:          +%d\n", diff)
	} else {
		fmt.Printf("difference:          %d\n", diff)
	}
	fmt.Printf("errors:              %d\n", e)
	fmt.Printf("elapsed:             %s\n", elapsed)
	if e > 0 {
		fmt.Println("WARNING: errors occurred — results may be incomplete")
	}
}
