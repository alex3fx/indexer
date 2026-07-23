// check_inner_phantom: checks which "inner phantom" addresses from Dune
// (type=create, success=false, tx_success=true, no real deploy at block<=N)
// actually exist in our Scylla contracts_by_address_v2 at block <= block-hi.
//
// These are addresses that inflate COUNT DISTINCT in Scylla vs Dune because
// our transformer checks outer tx.status (=1) but not trace-level success.
// Erigon trace API returns result.address for failed inner CREATE frames.
//
// Usage:
//   ./check_inner_phantom --pass=PASSWORD --addr-file=inner_phantom_addrs.txt --block-hi=25422000
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
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
	addrFile := flag.String("addr-file", "inner_phantom_addrs.txt", "file with addresses (one per line, 0x-prefixed or bare hex)")
	blockHi  := flag.Int64("block-hi", 25422000, "max block_number (inclusive)")
	workers  := flag.Int("workers", 32, "parallel Scylla query workers")
	outFile  := flag.String("out", "inner_phantom_in_scylla.txt", "output file for found addresses")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	// Load address list
	f, err := os.Open(*addrFile)
	if err != nil {
		log.Fatalf("open addr-file: %v", err)
	}
	var addrs []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// Normalise: ensure lowercase with 0x prefix
		line = strings.ToLower(line)
		if !strings.HasPrefix(line, "0x") {
			line = "0x" + line
		}
		addrs = append(addrs, line)
	}
	f.Close()
	log.Printf("Loaded %d addresses to check", len(addrs))

	// Connect to Scylla
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = "eth"
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// Check each address
	addrCh := make(chan string, 200)
	go func() {
		for _, a := range addrs {
			addrCh <- a
		}
		close(addrCh)
	}()

	var (
		checked   atomic.Int64
		found     atomic.Int64
		notFound  atomic.Int64
		errCount  atomic.Int64
	)

	var outMu sync.Mutex
	out, err := os.Create(*outFile)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer out.Close()
	outBuf := bufio.NewWriter(out)

	total := len(addrs)
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range addrCh {
				rowCount, err := countRows(session, addr, *blockHi)
				if err != nil {
					errCount.Add(1)
					log.Printf("[ERROR] %s: %v", addr, err)
				} else if rowCount > 0 {
					found.Add(1)
					outMu.Lock()
					fmt.Fprintf(outBuf, "%s\t%d\n", addr, rowCount)
					outMu.Unlock()
				} else {
					notFound.Add(1)
				}
				checked.Add(1)
			}
		}()
	}

	// Progress reporter
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c := checked.Load()
			if c >= int64(total) {
				return
			}
			elapsed := time.Since(start).Seconds()
			log.Printf("[progress] checked=%d/%d found=%d not_found=%d errors=%d elapsed=%.0fs",
				c, total, found.Load(), notFound.Load(), errCount.Load(), elapsed)
		}
	}()

	wg.Wait()
	outBuf.Flush()

	elapsed := time.Since(start)
	fmt.Printf("\n=== RESULT ===\n")
	fmt.Printf("addr-file:     %s\n", *addrFile)
	fmt.Printf("block_hi:      %d\n", *blockHi)
	fmt.Printf("total checked: %d\n", checked.Load())
	fmt.Printf("found in Scylla (block <= %d): %d\n", *blockHi, found.Load())
	fmt.Printf("not in Scylla: %d\n", notFound.Load())
	fmt.Printf("errors:        %d\n", errCount.Load())
	fmt.Printf("elapsed:       %s\n", elapsed.Round(time.Second))
	fmt.Printf("output file:   %s\n", *outFile)
	fmt.Printf("\nThese %d inner-phantom addresses exist in Scylla but not in Dune\n", found.Load())
	fmt.Printf("(outer tx succeeded, inner CREATE failed — Erigon still records result.address)\n")
}

// countRows returns the number of rows for addr at block <= blockHi.
// Retries up to 3 times on transient errors.
func countRows(session *gocql.Session, addr string, blockHi int64) (int64, error) {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			delay *= 2
		}
		n, err := tryCount(session, addr, blockHi)
		if err == nil {
			return n, nil
		}
		if attempt == 2 {
			return 0, err
		}
	}
	return 0, fmt.Errorf("unreachable")
}

func tryCount(session *gocql.Session, addr string, blockHi int64) (int64, error) {
	iter := session.Query(
		`SELECT block_number FROM eth.contracts_by_address_v2
		 WHERE address = ? AND block_number <= ?
		 ALLOW FILTERING`,
		addr, blockHi,
	).PageSize(50).Iter()

	var blockN int64
	var count int64
	for iter.Scan(&blockN) {
		count++
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	return count, nil
}
