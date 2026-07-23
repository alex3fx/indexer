// analyze_hist_phantom: finds addresses that have ONLY phantom rows in the historical range
// (block_number <= block-hi). These are addresses that inflate our COUNT DISTINCT vs Dune
// (Dune counts success=true traces; we stored phantom rows from failed txs).
//
// Algorithm:
//  1. Read phantom_v2.txt: build set of (address, block_number) pairs that are phantom
//     AND block <= block-hi.
//  2. For each address with at least one such pair, query Scylla for ALL its rows at
//     block <= block-hi.
//  3. If all returned rows are in the phantom set → this address has NO real deployment
//     in the historical range → it's a "historical phantom only" address and contributes
//     to the COUNT DISTINCT discrepancy with Dune.
//
// Usage:
//   ./analyze_hist_phantom --pass=PASSWORD --phantom-file=~/phantom_v2.txt --block-hi=25422000
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type phantomKey struct {
	addr    string
	blockN  int64
}

func main() {
	host       := flag.String("host", "127.0.0.1", "Scylla host")
	port       := flag.Int("port", 9042, "Scylla port")
	user       := flag.String("user", "cassandra", "user")
	pass       := flag.String("pass", "", "password")
	phantomFile := flag.String("phantom-file", "phantom_v2.txt", "path to phantom_v2.txt")
	blockHi    := flag.Int64("block-hi", 25422000, "historical range upper bound (inclusive)")
	workers    := flag.Int("workers", 32, "parallel Scylla query workers")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	// Step 1: load phantom_v2.txt
	log.Printf("Loading phantom file: %s", *phantomFile)
	phantomSet := make(map[phantomKey]struct{})
	histAddrs := make(map[string]struct{}) // addresses with >=1 phantom row at block <= blockHi

	f, err := os.Open(*phantomFile)
	if err != nil {
		log.Fatalf("open phantom file: %v", err)
	}
	sc := bufio.NewScanner(f)
	linesLoaded := 0
	for sc.Scan() {
		line := sc.Text()
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		addr := parts[0]
		class := parts[1]
		if class != "phantom" {
			continue
		}
		if len(parts) < 3 {
			continue
		}
		blockN, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}
		if blockN > *blockHi {
			continue
		}
		k := phantomKey{addr: addr, blockN: blockN}
		phantomSet[k] = struct{}{}
		histAddrs[addr] = struct{}{}
		linesLoaded++
	}
	f.Close()
	if err := sc.Err(); err != nil {
		log.Fatalf("scan phantom file: %v", err)
	}
	log.Printf("Loaded %d phantom rows at block <= %d, covering %d unique addresses", linesLoaded, *blockHi, len(histAddrs))

	// Step 2: connect to Scylla
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

	// Step 3: for each address, check if it has any non-phantom rows at block <= blockHi
	addrCh := make(chan string, 1000)
	go func() {
		for addr := range histAddrs {
			addrCh <- addr
		}
		close(addrCh)
	}()

	var (
		checked         atomic.Int64
		histPhantomOnly atomic.Int64 // addresses with ONLY phantom rows in historical range
		queryErrors     atomic.Int64
	)

	var outMu sync.Mutex
	outFile, err := os.Create("hist_phantom_only.txt")
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer outFile.Close()
	outBuf := bufio.NewWriter(outFile)

	start := time.Now()
	total := len(histAddrs)

	// Progress reporter
	go func() {
		for {
			time.Sleep(10 * time.Second)
			c := checked.Load()
			h := histPhantomOnly.Load()
			elapsed := time.Since(start).Seconds()
			rate := float64(c) / elapsed
			eta := float64(total-int(c)) / rate
			log.Printf("[progress] checked=%d/%d hist-phantom-only=%d errors=%d rate=%.0f/s eta=%.0fs",
				c, total, h, queryErrors.Load(), rate, eta)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range addrCh {
				isHistPhantomOnly, err := checkAddress(session, addr, *blockHi, phantomSet)
				if err != nil {
					queryErrors.Add(1)
				} else if isHistPhantomOnly {
					histPhantomOnly.Add(1)
					outMu.Lock()
					fmt.Fprintln(outBuf, addr)
					outMu.Unlock()
				}
				checked.Add(1)
			}
		}()
	}

	wg.Wait()
	outBuf.Flush()

	elapsed := time.Since(start)
	log.Printf("Done. checked=%d hist-phantom-only=%d errors=%d elapsed=%s",
		checked.Load(), histPhantomOnly.Load(), queryErrors.Load(), elapsed.Round(time.Second))

	fmt.Printf("\n=== RESULT ===\n")
	fmt.Printf("phantom-file:          %s\n", *phantomFile)
	fmt.Printf("block_hi:              %d\n", *blockHi)
	fmt.Printf("addrs with phantom <= block_hi: %d\n", len(histAddrs))
	fmt.Printf("hist-phantom-only:     %d\n", histPhantomOnly.Load())
	fmt.Printf("query errors:          %d\n", queryErrors.Load())
	fmt.Printf("elapsed:               %s\n", elapsed.Round(time.Second))
	fmt.Printf("output file:           hist_phantom_only.txt\n")
	fmt.Printf("\nThese %d addresses appear in our COUNT DISTINCT(block<==%d) but not in Dune\n",
		histPhantomOnly.Load(), *blockHi)
	fmt.Printf("because Dune counts success=true traces only.\n")
}

// checkAddress queries Scylla for all rows of this address at block <= blockHi.
// Returns true if ALL of those rows are phantom (i.e., all in phantomSet).
func checkAddress(session *gocql.Session, addr string, blockHi int64, phantomSet map[phantomKey]struct{}) (bool, error) {
	iter := session.Query(
		`SELECT block_number FROM eth.contracts_by_address_v2
		 WHERE address = ? AND block_number <= ?
		 ALLOW FILTERING`,
		addr, blockHi,
	).PageSize(100).Iter()

	var blockN int64
	hasRows := false
	for iter.Scan(&blockN) {
		hasRows = true
		k := phantomKey{addr: addr, blockN: blockN}
		if _, isPhantom := phantomSet[k]; !isPhantom {
			// This row is NOT phantom → address has at least one real row in historical range
			iter.Close()
			return false, nil
		}
	}
	if err := iter.Close(); err != nil {
		return false, err
	}
	// All rows (if any) were phantom
	return hasRows, nil
}
