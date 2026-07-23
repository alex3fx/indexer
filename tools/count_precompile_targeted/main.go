// count_precompile_targeted: independently verifies that reth traces direct (depth=0)
// precompile calls (0x01-0x0a) by querying Scylla for each specific block.
//
// Method: reads a list of blocks from --blocks file (one block number per line),
// queries internal_transactions per-block (not per-chunk), to avoid ALLOW FILTERING
// timeout on full chunk scans. Single-block queries are fast because Scylla narrows
// by partition key (chunk) + first clustering key (block_number) before filtering to_address.
//
// Usage: ./count_precompile_targeted --blocks blocks.txt --pass cassandra --to 25422400
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

var precompileAddrs = []string{
	"0x0000000000000000000000000000000000000001",
	"0x0000000000000000000000000000000000000002",
	"0x0000000000000000000000000000000000000003",
	"0x0000000000000000000000000000000000000004",
	"0x0000000000000000000000000000000000000005",
	"0x0000000000000000000000000000000000000006",
	"0x0000000000000000000000000000000000000007",
	"0x0000000000000000000000000000000000000008",
	"0x0000000000000000000000000000000000000009",
	"0x000000000000000000000000000000000000000a",
}

func blockToChunk(block, lanes, eraSize int64) int64 {
	lane := block % lanes
	era := block / eraSize
	return lane + lanes*era
}

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "port")
	user     := flag.String("user", "cassandra", "user")
	pass     := flag.String("pass", "", "password (required)")
	ks       := flag.String("keyspace", "eth", "keyspace")
	blocksF  := flag.String("blocks", "", "file with block numbers (one per line, required)")
	to       := flag.Int64("to", 25422400, "max block (inclusive)")
	workers  := flag.Int("workers", 32, "parallel workers")
	timeout  := flag.Duration("timeout", 30*time.Second, "query timeout per block")
	retries  := flag.Int("retries", 5, "retries per block")
	lanes    := flag.Int64("lanes", 24, "chunk lanes")
	eraSize  := flag.Int64("era-size", 12000, "blocks per era")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}
	if *blocksF == "" {
		log.Fatal("--blocks required (file with one block number per line)")
	}

	f, err := os.Open(*blocksF)
	if err != nil {
		log.Fatalf("open blocks file: %v", err)
	}
	var blocks []int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			log.Fatalf("bad block number %q: %v", line, err)
		}
		if n <= *to {
			blocks = append(blocks, n)
		}
	}
	f.Close()
	log.Printf("loaded %d blocks from %s (to=%d)", len(blocks), *blocksF, *to)

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *ks
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = *timeout
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	inList := "?"
	for i := 1; i < len(precompileAddrs); i++ {
		inList += ",?"
	}
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM internal_transactions WHERE chunk=? AND block_number=? AND to_address IN (%s) ALLOW FILTERING",
		inList,
	)

	blockCh := make(chan int64, len(blocks))
	for _, b := range blocks {
		blockCh <- b
	}
	close(blockCh)

	var (
		total    atomic.Int64
		errCount atomic.Int64
		done     atomic.Int64
		wg       sync.WaitGroup
	)
	start := time.Now()
	total_blocks := int64(len(blocks))

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for block := range blockCh {
				chunk := blockToChunk(block, *lanes, *eraSize)
				var cnt int64
				var lastErr error
				for attempt := 0; attempt <= *retries; attempt++ {
					if attempt > 0 {
						time.Sleep(time.Duration(attempt) * time.Second)
					}
					args := []interface{}{chunk, block}
					for _, addr := range precompileAddrs {
						args = append(args, addr)
					}
					if err := session.Query(q, args...).Scan(&cnt); err != nil {
						lastErr = err
						continue
					}
					lastErr = nil
					break
				}
				if lastErr != nil {
					log.Printf("ERROR block=%d chunk=%d: %v", block, chunk, lastErr)
					errCount.Add(1)
				} else {
					if cnt > 0 {
						log.Printf("block=%d chunk=%d precompile_rows=%d", block, chunk, cnt)
					}
					total.Add(cnt)
				}
				n := done.Add(1)
				if n%200 == 0 {
					log.Printf("progress: %d/%d blocks done, running_total=%d", n, total_blocks, total.Load())
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== COUNT PRECOMPILE ITX (targeted, %d blocks) ===\n", total_blocks)
	fmt.Printf("Precompile rows (0x01-0x0a, depth=0) in internal_transactions: %d\n", total.Load())
	fmt.Printf("Errors: %d  Elapsed: %.1fs\n", errCount.Load(), elapsed.Seconds())
}
