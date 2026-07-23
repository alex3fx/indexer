// count_block_list: counts TX/LOG/ITX rows in Scylla for a specific list of block numbers.
// Reads block numbers from --blocks file (one per line).
// Used to check whether specific blocks (e.g. missing-BC blocks) have their data in Scylla.
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

const (
	defaultLanes   = 24
	defaultEraSize = 12000
)

func chunkOf(block, lanes, eraSize int64) int {
	return int((block % lanes) + lanes*(block/eraSize))
}

func main() {
	host    := flag.String("host", "127.0.0.1", "Scylla host")
	port    := flag.Int("port", 9042, "port")
	user    := flag.String("user", "cassandra", "user")
	pass    := flag.String("pass", "", "password")
	ks      := flag.String("keyspace", "eth", "keyspace")
	blocks  := flag.String("blocks", "", "file with block numbers, one per line")
	workers := flag.Int("workers", 32, "parallel workers")
	timeout := flag.Duration("timeout", 60*time.Second, "query timeout")
	retries := flag.Int("retries", 5, "retries per query")
	lanes   := flag.Int64("lanes", defaultLanes, "chunk lanes")
	eraSize := flag.Int64("era-size", defaultEraSize, "blocks per era")
	flag.Parse()

	if *pass == "" { log.Fatal("--pass required") }
	if *blocks == "" { log.Fatal("--blocks required (file with block numbers)") }

	f, err := os.Open(*blocks)
	if err != nil { log.Fatalf("open %s: %v", *blocks, err) }
	var blockNums []int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" { continue }
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil { log.Printf("skip bad line: %q", line); continue }
		blockNums = append(blockNums, n)
	}
	f.Close()
	log.Printf("Loaded %d block numbers from %s", len(blockNums), *blocks)

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *ks
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = *timeout
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil { log.Fatalf("connect: %v", err) }
	defer session.Close()

	txQ  := `SELECT COUNT(*) FROM transactions          WHERE chunk=? AND block_number=?`
	lgQ  := `SELECT COUNT(*) FROM logs                  WHERE chunk=? AND block_number=?`
	itxQ := `SELECT COUNT(*) FROM internal_transactions  WHERE chunk=? AND block_number=?`

	retryCount := func(q string, chunk int, block int64) (int64, error) {
		var n int64
		var delay = 500 * time.Millisecond
		for attempt := 0; attempt < *retries; attempt++ {
			if attempt > 0 {
				time.Sleep(delay)
				if delay < 16*time.Second { delay *= 2 }
			}
			if err := session.Query(q, chunk, block).Scan(&n); err != nil {
				log.Printf("[retry %d/%d] block=%d: %v", attempt+1, *retries, block, err)
				continue
			}
			return n, nil
		}
		return 0, fmt.Errorf("all retries failed")
	}

	type result struct {
		block  int64
		tx     int64
		log    int64
		itx    int64
		hasErr bool
	}

	blockCh := make(chan int64, len(blockNums))
	for _, b := range blockNums { blockCh <- b }
	close(blockCh)

	resCh := make(chan result, len(blockNums))
	var wg sync.WaitGroup
	var done atomic.Int64
	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range blockCh {
				c := chunkOf(b, *lanes, *eraSize)
				tx,  e1 := retryCount(txQ,  c, b)
				lg,  e2 := retryCount(lgQ,  c, b)
				itx, e3 := retryCount(itxQ, c, b)
				hasErr := e1 != nil || e2 != nil || e3 != nil
				if hasErr {
					log.Printf("[ERROR] block %d: tx=%v log=%v itx=%v", b, e1, e2, e3)
				}
				resCh <- result{b, tx, lg, itx, hasErr}
				done.Add(1)
				if done.Load()%100 == 0 {
					log.Printf("progress: %d/%d blocks (%.0fs)", done.Load(), len(blockNums), time.Since(start).Seconds())
				}
			}
		}()
	}
	go func() { wg.Wait(); close(resCh) }()

	var totalTx, totalLog, totalItx int64
	var errCount int
	for r := range resCh {
		totalTx  += r.tx
		totalLog += r.log
		totalItx += r.itx
		if r.hasErr { errCount++ }
	}

	fmt.Printf("\n=== RESULTS: %d blocks from %s ===\n", len(blockNums), *blocks)
	fmt.Printf("%-20s  %12s  %12s  %12s\n", "metric", "transactions", "logs", "internal_txs")
	fmt.Printf("%-20s  %12d  %12d  %12d\n", "Scylla actual rows", totalTx, totalLog, totalItx)
	fmt.Printf("\nErrors: %d  Elapsed: %.1fs\n", errCount, time.Since(start).Seconds())
}
