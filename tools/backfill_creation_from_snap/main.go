// backfill_creation_from_snap: restores creation bytecode from the archived
// contracts_by_addresses snapshot into bytecode_store_v2 + contracts_by_address_v2.
//
// contracts_by_addresses: PK=(address), has creation_bytecode (hex text, 0x-prefixed).
//
// Per row with non-empty creation_bytecode:
//   1. hex-decode (strip 0x)
//   2. sha256  → bytecode_store_v2.hash
//   3. keccak256 → bytecode_store_v2.check_hash
//   4. INSERT INTO bytecode_store_v2 (hash, seq=0, ..., kind=1) IF NOT EXISTS
//   5. UPDATE contracts_by_address_v2 SET creation_hash=hash, creation_seq=0
//      WHERE address=? AND block_number=?
//
// Idempotent. Skips rows where creation_bytecode is empty or "0x".
//
// Usage: backfill_creation_from_snap --host 127.0.0.1 --pass cassandra [flags]
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/crypto/sha3"
)

type job struct {
	address     string
	blockNumber int64
	creation    []byte
}

func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

func sha256sum(data []byte) []byte {
	s := sha256.Sum256(data)
	return s[:]
}

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "Scylla user")
	pass     := flag.String("pass", "", "Scylla password")
	workers  := flag.Int("workers", 64, "concurrent write workers")
	pageSize := flag.Int("page-size", 500, "scan page size (keep low: rows are large)")
	dryRun   := flag.Bool("dry-run", false, "scan only, no writes")
	flag.Parse()

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalQuorum
	cluster.NumConns = 8
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	var (
		scanned     atomic.Int64
		skipped     atomic.Int64 // empty or invalid creation_bytecode
		insertedNew atomic.Int64 // new rows added to bytecode_store_v2
		alreadyHad  atomic.Int64 // IF NOT EXISTS returned not-applied
		updated     atomic.Int64 // contracts_by_address_v2 rows updated
		dbErr       atomic.Int64
	)

	start := time.Now()
	jobs := make(chan job, *workers*4)
	var wg sync.WaitGroup

	// INSERT IF NOT EXISTS into bytecode_store_v2 (kind=1 = creation bytecode).
	const insStmt = `INSERT INTO eth.bytecode_store_v2
		(hash, seq, bytecode, check_hash, size, kind, first_seen_block)
		VALUES (?, 0, ?, ?, ?, 1, ?)
		IF NOT EXISTS`

	// UPDATE creation_hash in contracts_by_address_v2.
	const updStmt = `UPDATE eth.contracts_by_address_v2
		SET creation_hash = ?, creation_seq = 0
		WHERE address = ? AND block_number = ?`

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if *dryRun {
					updated.Add(1)
					continue
				}

				hashBytes := sha256sum(j.creation)
				checkHash := keccak256(j.creation)
				size := len(j.creation)

				// INSERT IF NOT EXISTS — returns (applied=true) if row was new.
				applied, insErr := retryApplied(func() (bool, error) {
					m := map[string]interface{}{}
					return session.Query(insStmt,
						hashBytes, j.creation, checkHash, size, j.blockNumber,
					).MapScanCAS(m)
				})
				if insErr != nil {
					dbErr.Add(1)
					log.Printf("[err] insert bytecode_store_v2 addr=%s blk=%d: %v",
						j.address, j.blockNumber, insErr)
					continue
				}
				if applied {
					insertedNew.Add(1)
				} else {
					alreadyHad.Add(1)
				}

				// UPDATE contracts_by_address_v2.creation_hash.
				updErr := retryExec(func() error {
					return session.Query(updStmt,
						hashBytes, j.address, j.blockNumber,
					).Exec()
				})
				if updErr != nil {
					dbErr.Add(1)
					log.Printf("[err] update v2 addr=%s blk=%d: %v",
						j.address, j.blockNumber, updErr)
				} else {
					updated.Add(1)
				}
			}
		}()
	}

	// Progress reporter.
	go func() {
		for {
			time.Sleep(30 * time.Second)
			elapsed := time.Since(start)
			sc := scanned.Load()
			rate := float64(sc) / elapsed.Seconds()
			log.Printf("[progress] scanned=%d skipped=%d inserted_new=%d already_had=%d updated=%d dbErr=%d rate=%.0f rows/s elapsed=%s",
				sc, skipped.Load(), insertedNew.Load(), alreadyHad.Load(),
				updated.Load(), dbErr.Load(), rate, elapsed.Round(time.Second))
		}
	}()

	const sel = `SELECT address, block_number, creation_bytecode FROM eth.contracts_by_addresses`

	log.Printf("Starting scan (dry-run=%v, workers=%d, page-size=%d)...", *dryRun, *workers, *pageSize)
	iter := session.Query(sel).PageSize(*pageSize).Iter()

	var (
		address          string
		blockNumber      int64
		creationBytecode string
	)
	for iter.Scan(&address, &blockNumber, &creationBytecode) {
		scanned.Add(1)

		raw := strings.TrimPrefix(creationBytecode, "0x")
		raw = strings.TrimPrefix(raw, "0X")
		creationBytecode = "" // reset for next iter (gocql reuse)

		if raw == "" {
			skipped.Add(1)
			continue
		}

		decoded, decErr := hex.DecodeString(raw)
		if decErr != nil || len(decoded) == 0 {
			skipped.Add(1)
			continue
		}

		// Copy — gocql may reuse the underlying buffer across iterations.
		cp := make([]byte, len(decoded))
		copy(cp, decoded)

		jobs <- job{
			address:     strings.ToLower(address),
			blockNumber: blockNumber,
			creation:    cp,
		}
	}
	if err := iter.Close(); err != nil {
		log.Printf("[scan] iter error: %v", err)
	}

	close(jobs)
	wg.Wait()

	elapsed := time.Since(start).Round(time.Second)
	fmt.Println("=== DONE ===")
	fmt.Printf("scanned=%d  skipped=%d  inserted_new=%d  already_had=%d  updated=%d  dbErr=%d  elapsed=%s\n",
		scanned.Load(), skipped.Load(), insertedNew.Load(), alreadyHad.Load(),
		updated.Load(), dbErr.Load(), elapsed)
	if dbErr.Load() > 0 {
		fmt.Printf("WARNING: %d errors — re-run to retry\n", dbErr.Load())
	} else {
		fmt.Println("OK — no errors")
	}
}

func retryExec(fn func() error) error {
	const maxRetries = 8
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else if attempt == maxRetries-1 {
			return fmt.Errorf("after %d retries: %w", maxRetries, err)
		}
		time.Sleep(jitter(delay))
		delay = minDur(delay*2, 30*time.Second)
	}
	return nil
}

func retryApplied(fn func() (bool, error)) (bool, error) {
	const maxRetries = 8
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		applied, err := fn()
		if err == nil {
			return applied, nil
		}
		if attempt == maxRetries-1 {
			return false, fmt.Errorf("after %d retries: %w", maxRetries, err)
		}
		time.Sleep(jitter(delay))
		delay = minDur(delay*2, 30*time.Second)
	}
	return false, nil
}

func jitter(d time.Duration) time.Duration {
	// ±20% jitter via sin to avoid retry storms.
	v := time.Duration(float64(d) * (0.8 + 0.4*math.Sin(float64(time.Now().UnixNano()))))
	if v < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	return v
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
