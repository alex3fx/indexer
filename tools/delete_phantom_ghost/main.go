// delete_phantom_ghost: deletes phantom and ghost rows from contracts_by_address_v2.
//
// Phantom rows: deletes ONLY the specific (address, block_number) pairs listed in --phantom-file
//   (output of find_phantom_addrs). Does NOT delete by address wildcard — if the address was
//   successfully re-deployed at a new block_number since the scan, that row is preserved.
//
// Ghost rows: rows where tx_hash IS NULL. Found via full token-range scan.
//   Deletes only the specific (address, block_number) rows that have tx_hash=null.
//
// Modes:
//   --dry-run (default=true): logs what would be deleted, no actual deletes.
//   --dry-run=false: actually deletes.
//
// Safety:
//   - DELETE is idempotent (no-op if row already gone).
//   - All deletes are retried with exponential backoff (5 retries, max 30s delay).
//   - Persistent failures: logged and counted, never silently swallowed.
//   - Progress logged every 30s.
//   - On completion: prints summary of rows deleted vs failed.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	host        := flag.String("host", "127.0.0.1", "Scylla host")
	port        := flag.Int("port", 9042, "Scylla port")
	user        := flag.String("user", "cassandra", "user")
	pass        := flag.String("pass", "", "password")
	phantomFile := flag.String("phantom-file", "", "path to phantom_addresses.txt (find_phantom_addrs output)")
	segments    := flag.Int("segments", 256, "token-range segments for ghost scan")
	pageSize    := flag.Int("page-size", 1000, "CQL page size for ghost scan")
	dryRun      := flag.Bool("dry-run", true, "if true, log deletes but don't execute")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 8
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	if *dryRun {
		log.Println("=== DRY RUN MODE — no actual deletes ===")
	} else {
		log.Println("=== LIVE MODE — deletes will be executed ===")
	}

	var (
		phantomRowsDeleted atomic.Int64
		ghostRowsDeleted   atomic.Int64
		deleteErrors       atomic.Int64
	)

	// Phase 1: delete phantom address rows
	if *phantomFile != "" {
		log.Printf("Phase 1: deleting phantom addresses from %s", *phantomFile)
		phantomAddrs, err := loadPhantomAddresses(*phantomFile)
		if err != nil {
			log.Fatalf("load phantom file: %v", err)
		}
		log.Printf("Loaded %d phantom addresses", len(phantomAddrs))

		// Delete only the (address, block_number) pairs confirmed phantom at scan time.
		deleted := deletePhantomAddresses(session, phantomAddrs, *dryRun, &deleteErrors)
		phantomRowsDeleted.Store(int64(deleted))
		log.Printf("Phase 1 complete: %d phantom rows deleted (%d errors)", deleted, deleteErrors.Load())
	} else {
		log.Println("Phase 1 skipped: --phantom-file not provided")
	}

	// Phase 2: delete ghost rows (tx_hash IS NULL) via token-range scan
	log.Printf("Phase 2: scanning for ghost rows (tx_hash=null) across %d segments", *segments)
	ghostDeleted := deleteGhostRows(session, *segments, *pageSize, *dryRun, &ghostRowsDeleted, &deleteErrors)
	_ = ghostDeleted

	// Summary
	fmt.Println("=== SUMMARY ===")
	if *dryRun {
		fmt.Println("DRY RUN — no rows were actually deleted")
	}
	fmt.Printf("phantom rows deleted: %d\n", phantomRowsDeleted.Load())
	fmt.Printf("ghost rows deleted:   %d\n", ghostRowsDeleted.Load())
	fmt.Printf("delete errors:        %d\n", deleteErrors.Load())
	if deleteErrors.Load() > 0 {
		log.Println("WARNING: delete errors occurred — some rows may not have been deleted. Re-run to retry.")
	}
}

// loadPhantomAddresses reads the phantom_addresses.txt file.
// Format: address\tphantom\tblock_number\ttx_hash   (phantom rows)
//         address\tghost-only\n                      (ghost-only rows, skip)
// Returns map[address][]blockNumber for phantom entries.
//
// IMPORTANT: we use block_numbers from the FILE, not re-read from DB.
// This ensures we only delete the specific rows confirmed phantom at scan time.
// If the address was later successfully deployed at a new block_number (CREATE2 retry),
// that new block_number is not in this file and will NOT be deleted.
func loadPhantomAddresses(path string) (map[string][]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string][]int64)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		addr := parts[0]
		kind := parts[1]
		if kind == "ghost-only" {
			// ghost-only addresses: their rows have tx_hash=null, caught by phase 2
			continue
		}
		if kind != "phantom" || len(parts) < 3 {
			continue
		}
		var blockN int64
		fmt.Sscanf(parts[2], "%d", &blockN)
		result[addr] = append(result[addr], blockN)
	}
	return result, scanner.Err()
}

// deletePhantomAddresses deletes ONLY the specific (address, block_number) rows listed in
// the phantom file. We do NOT re-read all rows from DB because:
//   - The address may have been successfully re-deployed at a NEW block_number since the scan.
//   - Only rows confirmed phantom at scan time (tx.status=0) should be deleted.
//   - Tx status cannot flip from 0 to 1 on ETH mainnet after finalization.
func deletePhantomAddresses(
	session *gocql.Session,
	addrs map[string][]int64,
	dryRun bool,
	deleteErrors *atomic.Int64,
) int {
	start := time.Now()
	var mu sync.Mutex
	totalDeleted := 0
	totalFailed := 0
	processed := 0
	total := len(addrs)

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				p, d := processed, totalDeleted
				mu.Unlock()
				elapsed := time.Since(start)
				log.Printf("[phantom-delete progress] %d/%d addrs, %d rows deleted, elapsed=%s",
					p, total, d, elapsed.Round(time.Second))
			case <-done:
				return
			}
		}
	}()

	// Process concurrently with bounded parallelism
	sem := make(chan struct{}, 32)
	var wg sync.WaitGroup

	for addr, blocks := range addrs {
		addr := addr
		blocks := blocks
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			// Delete ONLY the specific (address, block_number) pairs from the file.
			deleted := 0
			failed := 0
			for _, bn := range blocks {
				if dryRun {
					log.Printf("[dry-run] DELETE contracts_by_address_v2 WHERE address=%s AND block_number=%d", addr, bn)
					deleted++
					continue
				}
				ok := retryWithBackoff(func() error {
					return session.Query(
						`DELETE FROM eth.contracts_by_address_v2 WHERE address=? AND block_number=?`,
						addr, bn,
					).Exec()
				}, 5, addr, deleteErrors)
				if ok {
					deleted++
				} else {
					failed++
				}
			}

			mu.Lock()
			processed++
			totalDeleted += deleted
			totalFailed += failed
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(done)
	elapsed := time.Since(start).Round(time.Second)
	log.Printf("[phantom-delete] done: %d addrs, %d rows deleted, %d failed, elapsed=%s",
		total, totalDeleted, totalFailed, elapsed)
	return totalDeleted
}

// deleteGhostRows scans contracts_by_address_v2 for rows with tx_hash=null and deletes them.
func deleteGhostRows(
	session *gocql.Session,
	numSegments, pageSize int,
	dryRun bool,
	ghostRowsDeleted, deleteErrors *atomic.Int64,
) int64 {
	start := time.Now()
	n := numSegments
	totalF := float64(uint64(math.MaxUint64)) + 1.0
	stepF := totalF / float64(n)

	// Progress ticker
	go func() {
		for {
			time.Sleep(30 * time.Second)
			elapsed := time.Since(start)
			log.Printf("[ghost-delete progress] deleted=%d errors=%d elapsed=%s",
				ghostRowsDeleted.Load(), deleteErrors.Load(), elapsed.Round(time.Second))
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
			scanAndDeleteGhosts(session, lo, hi, last, pageSize, dryRun, ghostRowsDeleted, deleteErrors)
		}(lo, hi, last)
	}
	wg.Wait()

	elapsed := time.Since(start).Round(time.Second)
	log.Printf("[ghost-delete] done: %d ghost rows deleted, %d errors, elapsed=%s",
		ghostRowsDeleted.Load(), deleteErrors.Load(), elapsed)
	return ghostRowsDeleted.Load()
}

func scanAndDeleteGhosts(
	session *gocql.Session,
	lo, hi int64, last bool, pageSize int,
	dryRun bool,
	ghostRowsDeleted, deleteErrors *atomic.Int64,
) {
	var q *gocql.Query
	if last {
		q = session.Query(
			`SELECT address, block_number, tx_hash FROM eth.contracts_by_address_v2
			 WHERE token(address) >= ?`,
			lo,
		)
	} else {
		q = session.Query(
			`SELECT address, block_number, tx_hash FROM eth.contracts_by_address_v2
			 WHERE token(address) >= ? AND token(address) < ?`,
			lo, hi,
		)
	}

	iter := q.PageSize(pageSize).Iter()
	var addr, txHash string
	var blockN int64

	for iter.Scan(&addr, &blockN, &txHash) {
		if txHash != "" {
			continue // not a ghost row
		}
		if dryRun {
			log.Printf("[dry-run] DELETE ghost WHERE address=%s AND block_number=%d", addr, blockN)
			ghostRowsDeleted.Add(1)
			continue
		}
		ok := retryWithBackoff(func() error {
			return session.Query(
				`DELETE FROM eth.contracts_by_address_v2 WHERE address=? AND block_number=?`,
				addr, blockN,
			).Exec()
		}, 5, addr, deleteErrors)
		if ok {
			ghostRowsDeleted.Add(1)
		}
	}

	if err := iter.Close(); err != nil {
		log.Printf("[ghost-delete] scan error segment lo=%d: %v", lo, err)
		deleteErrors.Add(1)
	}
}

// retryWithBackoff retries fn up to maxRetries times with exponential backoff.
// Returns true on success, false on persistent failure.
// On failure: increments deleteErrors and logs (does NOT skip silently).
func retryWithBackoff(fn func() error, maxRetries int, addr string, deleteErrors *atomic.Int64) bool {
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return true
		}
		if attempt == maxRetries {
			log.Printf("[delete-error] addr=%s failed after %d retries: %v", addr, maxRetries, err)
			deleteErrors.Add(1)
			return false
		}
		time.Sleep(backoff)
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	return false // unreachable
}
