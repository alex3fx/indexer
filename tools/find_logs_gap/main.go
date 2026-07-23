// find_logs_gap: compares block_completions.log_count vs actual logs table per block.
// Reports blocks where actual log count < expected (missing logs).
//
// Algorithm per chunk:
//   1. SELECT block_number, log_count FROM block_completions WHERE chunk=? AND block_number<=?
//   2. SELECT block_number, COUNT(*) FROM logs WHERE chunk=? AND block_number<=? GROUP BY block_number
//   3. For each block with expected>0: if actual < expected → missing logs
//
// Output: missing_logs.tsv  (block_number \t expected \t actual \t missing)
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

const (
	defaultLanes   = 24
	defaultEraSize = 12000
)

type BlockDiff struct {
	Block    int64
	Expected int64
	Actual   int64
}

func main() {
	host       := flag.String("host", "127.0.0.1", "Scylla host")
	port       := flag.Int("port", 9042, "Scylla port")
	user       := flag.String("user", "cassandra", "user")
	pass       := flag.String("pass", "", "password")
	keyspace   := flag.String("keyspace", "eth", "keyspace")
	checkpoint := flag.Int64("checkpoint", 0, "fixed block number (inclusive upper bound)")
	workers    := flag.Int("workers", 16, "parallel workers")
	outFile    := flag.String("out", "missing_logs.tsv", "output file")
	timeout    := flag.Duration("timeout", 120*time.Second, "Scylla query timeout")
	retries    := flag.Int("retries", 8, "retry attempts per chunk")
	lanes      := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize    := flag.Int64("era-size", defaultEraSize, "blocks per era")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}
	if *checkpoint == 0 {
		log.Fatal("--checkpoint required")
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

	maxEra := *checkpoint / *eraSize
	var chunks []int
	for era := int64(0); era <= maxEra; era++ {
		for lane := 0; lane < *lanes; lane++ {
			chunks = append(chunks, lane+*lanes*int(era))
		}
	}
	log.Printf("checkpoint=%d  chunks=%d  workers=%d  timeout=%s  retries=%d", *checkpoint, len(chunks), *workers, *timeout, *retries)

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var mu sync.Mutex
	var allDiffs []BlockDiff
	var chunksOK, chunksErr atomic.Int64
	var totalMissing, totalExtra atomic.Int64
	start := time.Now()

	logTicker := time.NewTicker(30 * time.Second)
	stopLog := make(chan struct{})
	go func() {
		for {
			select {
			case <-logTicker.C:
				done := chunksOK.Load() + chunksErr.Load()
				log.Printf("chunks=%d/%d  ok=%d  err=%d  missing_blocks=%d  total_missing_logs=%d  elapsed=%.0fs",
					done, len(chunks), chunksOK.Load(), chunksErr.Load(),
					int64(len(allDiffs)), totalMissing.Load(), time.Since(start).Seconds())
			case <-stopLog:
				logTicker.Stop()
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				diffs, extra, err := processChunk(session, chunk, *checkpoint, *retries)
				if err != nil {
					chunksErr.Add(1)
					log.Printf("[ERROR] chunk=%d: %v", chunk, err)
					continue
				}
				chunksOK.Add(1)
				if len(diffs) > 0 || extra > 0 {
					mu.Lock()
					allDiffs = append(allDiffs, diffs...)
					mu.Unlock()
					totalMissing.Add(func() int64 {
						var s int64
						for _, d := range diffs {
							s += d.Expected - d.Actual
						}
						return s
					}())
					totalExtra.Add(extra)
				}
			}
		}()
	}
	wg.Wait()
	close(stopLog)

	elapsed := time.Since(start)

	sort.Slice(allDiffs, func(i, j int) bool {
		return allDiffs[i].Block < allDiffs[j].Block
	})

	f, err := os.Create(*outFile)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "block_number\texpected_logs\tactual_logs\tmissing")
	var fileTotalMissing int64
	for _, d := range allDiffs {
		missing := d.Expected - d.Actual
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\n", d.Block, d.Expected, d.Actual, missing)
		fileTotalMissing += missing
	}
	w.Flush()

	fmt.Printf("\n=== RESULTS ===\n")
	fmt.Printf("Elapsed:              %.1fs\n", elapsed.Seconds())
	fmt.Printf("Chunks OK:            %d\n", chunksOK.Load())
	fmt.Printf("Chunks ERROR:         %d\n", chunksErr.Load())
	fmt.Printf("Blocks with missing:  %d\n", len(allDiffs))
	fmt.Printf("Total missing logs:   %d\n", fileTotalMissing)
	fmt.Printf("Total extra logs:     %d\n", totalExtra.Load())
	fmt.Printf("Output:               %s\n", *outFile)
}

// processChunk compares block_completions vs actual logs for one chunk.
// Returns blocks with missing logs and count of extra (orphaned) logs.
func processChunk(session *gocql.Session, chunk int, checkpoint int64, maxRetries int) ([]BlockDiff, int64, error) {
	// Step 1: load expected from block_completions
	expected := make(map[int64]int64)
	err := retryQuery(maxRetries, func() error {
		iter := session.Query(
			`SELECT block_number, log_count FROM block_completions WHERE chunk=? AND block_number<=?`,
			chunk, checkpoint,
		).Iter()
		var bn int64
		var lc int
		for iter.Scan(&bn, &lc) {
			if lc > 0 {
				expected[bn] = int64(lc)
			}
		}
		return iter.Close()
	})
	if err != nil {
		return nil, 0, fmt.Errorf("block_completions: %w", err)
	}

	if len(expected) == 0 {
		return nil, 0, nil
	}

	// Step 2: load actual from logs using GROUP BY block_number
	actual := make(map[int64]int64)
	err = retryQuery(maxRetries, func() error {
		actual = make(map[int64]int64)
		iter := session.Query(
			`SELECT block_number, COUNT(*) FROM logs WHERE chunk=? AND block_number<=? GROUP BY block_number`,
			chunk, checkpoint,
		).Iter()
		var bn, cnt int64
		for iter.Scan(&bn, &cnt) {
			actual[bn] = cnt
		}
		return iter.Close()
	})
	if err != nil {
		// GROUP BY might not be supported; fall back to row scan
		actual, err = countLogsByBlock(session, chunk, checkpoint, maxRetries)
		if err != nil {
			return nil, 0, fmt.Errorf("logs count: %w", err)
		}
	}

	// Step 3: compare
	var diffs []BlockDiff
	var extra int64
	for bn, exp := range expected {
		act := actual[bn]
		if act < exp {
			diffs = append(diffs, BlockDiff{Block: bn, Expected: exp, Actual: act})
		}
	}
	// count extra logs in blocks not in block_completions
	for bn, act := range actual {
		if expected[bn] == 0 {
			extra += act
		}
	}
	return diffs, extra, nil
}

// countLogsByBlock is the fallback: scans all rows and counts per block_number
func countLogsByBlock(session *gocql.Session, chunk int, checkpoint int64, maxRetries int) (map[int64]int64, error) {
	counts := make(map[int64]int64)
	err := retryQuery(maxRetries, func() error {
		counts = make(map[int64]int64)
		iter := session.Query(
			`SELECT block_number FROM logs WHERE chunk=? AND block_number<=?`,
			chunk, checkpoint,
		).PageSize(5000).Iter()
		var bn int64
		for iter.Scan(&bn) {
			counts[bn]++
		}
		return iter.Close()
	})
	return counts, err
}

func retryQuery(maxRetries int, fn func() error) error {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		if err := fn(); err == nil {
			return nil
		} else if attempt == maxRetries-1 {
			return err
		}
	}
	return fmt.Errorf("all %d retries exhausted", maxRetries)
}
