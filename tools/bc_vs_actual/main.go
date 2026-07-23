// bc_vs_actual: compares block_completions.tx_count vs actual transaction row counts per block.
// For each block in the range: fetches BC.tx_count and COUNT(*) from transactions,
// reports blocks where they differ, blocks missing from BC, and blocks missing from transactions.
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

const (
	defaultLanes   = 24
	defaultEraSize = 12000
)

type blockDiff struct {
	blockNum int64
	bcCount  int64 // -1 if missing from block_completions
	txCount  int64 // 0 if no rows in transactions
}

func main() {
	host      := flag.String("host", "127.0.0.1", "Scylla host")
	port      := flag.Int("port", 9042, "port")
	user      := flag.String("user", "cassandra", "user")
	pass      := flag.String("pass", "", "password")
	keyspace  := flag.String("keyspace", "eth", "keyspace")
	from      := flag.Int64("from", 0, "start block (inclusive)")
	to        := flag.Int64("to", 0, "end block (inclusive)")
	workers   := flag.Int("workers", 8, "parallel workers")
	timeout   := flag.Duration("timeout", 120*time.Second, "query timeout")
	retries   := flag.Int("retries", 5, "retries per chunk")
	lanes     := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize   := flag.Int64("era-size", defaultEraSize, "blocks per era")
	maxReport := flag.Int("max-report", 100, "max discrepancies to print")
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

	type result struct {
		diffs []blockDiff
		errs  int
	}

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	resultCh := make(chan result, len(chunks))

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				diffs, errs := processChunk(session, chunk, *from, *to, *retries)
				resultCh <- result{diffs, errs}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var allDiffs []blockDiff
	totalErrs := 0
	for r := range resultCh {
		allDiffs = append(allDiffs, r.diffs...)
		totalErrs += r.errs
	}

	sort.Slice(allDiffs, func(i, j int) bool {
		return allDiffs[i].blockNum < allDiffs[j].blockNum
	})

	missingBC := 0    // has tx rows but no BC entry
	wrongCount := 0   // BC.tx_count != actual count
	missingTx := 0    // BC entry exists but no tx rows

	for _, d := range allDiffs {
		switch {
		case d.bcCount == -1:
			missingBC++
		case d.txCount == 0:
			missingTx++
		default:
			wrongCount++
		}
	}

	fmt.Printf("\n=== RESULTS from=%d to=%d ===\n", *from, *to)
	fmt.Printf("Blocks with mismatching BC vs actual: %d\n", len(allDiffs))
	fmt.Printf("  missing from block_completions:     %d\n", missingBC)
	fmt.Printf("  BC.tx_count != actual count:        %d\n", wrongCount)
	fmt.Printf("  BC entry but 0 tx rows:             %d\n", missingTx)
	fmt.Printf("Chunk errors:                         %d\n", totalErrs)

	if len(allDiffs) > 0 {
		fmt.Printf("\nSample discrepancies (up to %d):\n", *maxReport)
		fmt.Printf("%-12s  %10s  %10s  %10s\n", "block", "BC.tx_count", "actual", "diff")
		printed := 0
		for _, d := range allDiffs {
			if printed >= *maxReport {
				break
			}
			bcStr := fmt.Sprintf("%d", d.bcCount)
			if d.bcCount == -1 {
				bcStr = "MISSING"
			}
			diff := d.txCount - d.bcCount
			if d.bcCount == -1 {
				diff = 0
			}
			fmt.Printf("%-12d  %10s  %10d  %10d\n", d.blockNum, bcStr, d.txCount, diff)
			printed++
		}
	}
}

func processChunk(session *gocql.Session, chunk int, from, to int64, maxRetries int) ([]blockDiff, int) {
	// Fetch BC tx_counts for this chunk
	bcMap := make(map[int64]int64)
	var errCount int

	bcQuery := `SELECT block_number, tx_count FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?`
	if err := retryIter(session, bcQuery, chunk, from, to, maxRetries, func(iter *gocql.Iter) {
		var bn, tc int64
		for iter.Scan(&bn, &tc) {
			bcMap[bn] = tc
		}
	}); err != nil {
		log.Printf("[ERROR] BC chunk=%d: %v", chunk, err)
		errCount++
		return nil, errCount
	}

	// Fetch all block_numbers from transactions and group in memory (GROUP BY times out on wide partitions)
	txMap := make(map[int64]int64)
	txQuery := `SELECT block_number FROM transactions WHERE chunk=? AND block_number>=? AND block_number<=?`
	if err := retryIter(session, txQuery, chunk, from, to, maxRetries, func(iter *gocql.Iter) {
		var bn int64
		for iter.Scan(&bn) {
			txMap[bn]++
		}
	}); err != nil {
		log.Printf("[ERROR] TX chunk=%d: %v", chunk, err)
		errCount++
		return nil, errCount
	}

	// Find discrepancies
	var diffs []blockDiff

	// Blocks in tx but not in BC, or BC count wrong
	for bn, txCnt := range txMap {
		bcCnt, inBC := bcMap[bn]
		if !inBC {
			diffs = append(diffs, blockDiff{bn, -1, txCnt})
		} else if bcCnt != txCnt {
			diffs = append(diffs, blockDiff{bn, bcCnt, txCnt})
		}
	}

	// Blocks in BC but not in tx
	for bn, bcCnt := range bcMap {
		if _, inTx := txMap[bn]; !inTx {
			diffs = append(diffs, blockDiff{bn, bcCnt, 0})
		}
	}

	return diffs, errCount
}

func retryIter(session *gocql.Session, query string, chunk int, from, to int64, maxRetries int, fn func(*gocql.Iter)) error {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 32*time.Second {
				delay *= 2
			}
		}
		iter := session.Query(query, chunk, from, to).Iter()
		fn(iter)
		if err := iter.Close(); err != nil {
			log.Printf("[retry %d/%d] chunk=%d: %v", attempt+1, maxRetries, chunk, err)
			continue
		}
		return nil
	}
	return fmt.Errorf("all %d retries failed (chunk=%d)", maxRetries, chunk)
}
