// check_gap_phantoms: finds and deletes phantom contract rows that entered the DB
// between the cleanup run (2026-07-13) and the transformer fix deployment (raw_erc20_v20,
// 2026-07-14). During this window, v19 (without the status=0 fix) indexed blocks
// ~25,459,261–25,525,803, potentially recording contracts from failed transactions.
//
// Strategy (two phases):
//   Phase 1 — build a set of failed-tx hashes for the block range by querying the
//     chunk-partitioned `transactions` table (168 chunk partitions for eras 2121–2127).
//     Fast: no full scan needed.
//
//   Phase 2 — full 256-segment token-range scan of contracts_by_address_v2.
//     For each row where block_number is in [from-block, to-block] AND tx_hash is in
//     the failed-tx set → phantom row. Delete from contracts_by_address_v2.
//
// Modes:
//   --dry-run (default=true): report phantoms, no actual deletes.
//   --dry-run=false: delete phantom rows from contracts_by_address_v2.
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
	host      := flag.String("host", "127.0.0.1", "Scylla host")
	port      := flag.Int("port", 9042, "Scylla port")
	user      := flag.String("user", "cassandra", "user")
	pass      := flag.String("pass", "", "password")
	keyspace  := flag.String("keyspace", "eth", "keyspace")
	fromBlock := flag.Int64("from-block", 25459261, "start block (inclusive)")
	toBlock   := flag.Int64("to-block", 25525803, "end block (inclusive)")
	segments  := flag.Int("segments", 256, "token-range segments for phase 2 scan")
	pageSize  := flag.Int("page-size", 1000, "CQL page size for phase 2 scan")
	workers   := flag.Int("workers", 24, "parallel chunk workers for phase 1")
	scanWorks := flag.Int("scan-workers", 64, "parallel segment workers for phase 2")
	dryRun    := flag.Bool("dry-run", true, "if true, report but don't delete")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 60 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 8
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	if *dryRun {
		log.Println("=== DRY RUN — no deletes will be executed ===")
	} else {
		log.Println("=== LIVE MODE — deletes will be executed ===")
	}

	// ── Phase 1: collect failed-tx hashes from transactions table ────────────────
	log.Printf("Phase 1: collecting failed tx hashes for blocks [%d, %d]...", *fromBlock, *toBlock)

	startEra := *fromBlock / 12000
	endEra   := *toBlock / 12000
	chunks := []int{}
	for era := startEra; era <= endEra; era++ {
		for lane := 0; lane < 24; lane++ {
			chunks = append(chunks, lane+24*int(era))
		}
	}
	log.Printf("  querying %d chunks (eras %d–%d)", len(chunks), startEra, endEra)

	var failedMu sync.Mutex
	failedHashes := make(map[string]bool, 1024)

	chunkWork := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkWork <- c
	}
	close(chunkWork)

	var wg1 sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			for chunk := range chunkWork {
				iter := session.Query(
					`SELECT hash, status FROM transactions WHERE chunk=? AND block_number>=? AND block_number<=?`,
					chunk, *fromBlock, *toBlock,
				).PageSize(5000).Iter()
				var hash string
				var status int8
				var localFailed []string
				for iter.Scan(&hash, &status) {
					if status == 0 {
						localFailed = append(localFailed, hash)
					}
				}
				if err := iter.Close(); err != nil {
					log.Printf("[phase1] chunk %d error: %v", chunk, err)
				}
				if len(localFailed) > 0 {
					failedMu.Lock()
					for _, h := range localFailed {
						failedHashes[h] = true
					}
					failedMu.Unlock()
				}
			}
		}()
	}
	wg1.Wait()
	log.Printf("Phase 1 done: %d unique failed-tx hashes found", len(failedHashes))

	if len(failedHashes) == 0 {
		fmt.Println("=== SUMMARY ===")
		fmt.Printf("block range:        [%d, %d]\n", *fromBlock, *toBlock)
		fmt.Println("failed tx hashes:   0 — no phantoms possible")
		fmt.Println("phantom rows found: 0")
		if *dryRun {
			fmt.Println("DRY RUN — no rows deleted")
		} else {
			fmt.Println("phantom rows deleted: 0")
			fmt.Println("delete errors:        0")
		}
		return
	}

	// ── Phase 2: token-range scan of contracts_by_address_v2 ────────────────────
	log.Printf("Phase 2: scanning contracts_by_address_v2 (%d segments)...", *segments)

	tokenMin := int64(math.MinInt64)
	tokenMax := int64(math.MaxInt64)
	step := uint64(tokenMax-tokenMin) / uint64(*segments)

	type segWork struct{ lo, hi int64 }
	segCh := make(chan segWork, *segments)
	for i := 0; i < *segments; i++ {
		lo := tokenMin + int64(uint64(i)*step)
		var hi int64
		if i == *segments-1 {
			hi = tokenMax
		} else {
			hi = tokenMin + int64(uint64(i+1)*step) - 1
		}
		segCh <- segWork{lo, hi}
	}
	close(segCh)

	var (
		phantomFound   atomic.Int64
		phantomDeleted atomic.Int64
		deleteErrors   atomic.Int64
		segsProcessed  atomic.Int64
	)

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("[phase2 progress] segs=%d/%d phantoms=%d deleted=%d errors=%d",
					segsProcessed.Load(), *segments,
					phantomFound.Load(), phantomDeleted.Load(), deleteErrors.Load())
			case <-done:
				return
			}
		}
	}()

	var wg2 sync.WaitGroup
	for i := 0; i < *scanWorks; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for seg := range segCh {
				iter := session.Query(
					`SELECT address, block_number, tx_hash FROM contracts_by_address_v2
					 WHERE token(address) >= ? AND token(address) <= ?`,
					seg.lo, seg.hi,
				).PageSize(*pageSize).Iter()

				var addr, txHash string
				var bn int64
				for iter.Scan(&addr, &bn, &txHash) {
					if bn < *fromBlock || bn > *toBlock {
						continue
					}
					if !failedHashes[txHash] {
						continue
					}
					// Phantom row: contract from a failed tx
					phantomFound.Add(1)
					if *dryRun {
						log.Printf("[dry-run] DELETE contracts_by_address_v2 WHERE address=%s AND block_number=%d (tx_hash=%s)", addr, bn, txHash)
					} else {
						ok := retryDelete(session, addr, bn)
						if ok {
							phantomDeleted.Add(1)
						} else {
							deleteErrors.Add(1)
							log.Printf("[ERROR] persistent delete failure: address=%s block=%d", addr, bn)
						}
					}
				}
				if err := iter.Close(); err != nil {
					log.Printf("[phase2] segment [%d,%d] error: %v", seg.lo, seg.hi, err)
				}
				segsProcessed.Add(1)
			}
		}()
	}

	wg2.Wait()
	close(done)

	fmt.Println("=== SUMMARY ===")
	fmt.Printf("block range:        [%d, %d]\n", *fromBlock, *toBlock)
	fmt.Printf("failed tx hashes:   %d\n", len(failedHashes))
	fmt.Printf("phantom rows found: %d\n", phantomFound.Load())
	if *dryRun {
		fmt.Println("DRY RUN — no rows deleted")
	} else {
		fmt.Printf("phantom rows deleted: %d\n", phantomDeleted.Load())
		fmt.Printf("delete errors:        %d\n", deleteErrors.Load())
	}
}

func retryDelete(session *gocql.Session, address string, blockNumber int64) bool {
	var delay time.Duration = 200 * time.Millisecond
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 30*time.Second {
				delay *= 2
			}
		}
		err := session.Query(
			`DELETE FROM contracts_by_address_v2 WHERE address=? AND block_number=?`,
			address, blockNumber,
		).Exec()
		if err == nil {
			return true
		}
		log.Printf("[retry %d] delete %s/%d: %v", attempt+1, address, blockNumber, err)
	}
	return false
}
