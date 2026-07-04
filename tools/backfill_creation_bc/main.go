// backfill_creation_bc: populates addresses_by_creation_bytecode from contracts_by_address_v2.
//
// Reads (address, block_number, creation_hash, creation_seq) from contracts_by_address_v2
// via full token-range scan (no ALLOW FILTERING), writes to addresses_by_creation_bytecode.
// Skips rows where creation_hash is all-zero (precompiles / old rows before tracking).
// Safe to re-run: INSERT is idempotent (same PK = upsert).
//
// Usage:
//   ./backfill_creation_bc --host=127.0.0.1 --pass=cassandra [--workers=256] [--log-every=100000]
package main

import (
	"encoding/hex"
	"flag"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

var zeroHash = make([]byte, 32)

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "Scylla username")
	pass     := flag.String("pass", "", "Scylla password")
	keyspace := flag.String("keyspace", "eth", "Scylla keyspace")
	workers  := flag.Int("workers", 256, "write concurrency")
	logEvery := flag.Int64("log-every", 100_000, "log progress every N rows")
	pageSize := flag.Int("page-size", 5000, "CQL fetch page size")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass is required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 16
	cluster.PageSize = *pageSize
	cluster.Timeout = 120 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	const insQ = `INSERT INTO addresses_by_creation_bytecode (hash, seq, bucket, address, block_number) VALUES (?, ?, ?, ?, ?)`

	type work struct {
		hash     []byte
		seq      int8
		bucket   int16
		address  string
		blockNum int64
	}

	ch := make(chan work, *workers*4)

	// Writer goroutines.
	var written, errors atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range ch {
				err := session.Query(insQ, w.hash, w.seq, w.bucket, w.address, w.blockNum).Exec()
				if err != nil {
					errors.Add(1)
					log.Printf("[err] INSERT addr=%s block=%d: %v", w.address, w.blockNum, err)
				} else {
					written.Add(1)
				}
			}
		}()
	}

	// Progress reporter.
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(30 * time.Second):
				log.Printf("[progress] written=%d errors=%d", written.Load(), errors.Load())
			}
		}
	}()

	// Full table scan of contracts_by_address_v2 (no ALLOW FILTERING needed for full scan).
	log.Printf("Starting full scan of contracts_by_address_v2 (page_size=%d, write_workers=%d)", *pageSize, *workers)
	t0 := time.Now()

	var scanned int64
	iter := session.Query(
		`SELECT address, block_number, creation_hash, creation_seq FROM contracts_by_address_v2`,
	).PageSize(*pageSize).Iter()

	var address string
	var blockNum int64
	var creationHash []byte
	var creationSeq int8

	for iter.Scan(&address, &blockNum, &creationHash, &creationSeq) {
		scanned++
		if scanned%*logEvery == 0 {
			log.Printf("[scan] scanned=%d written=%d errors=%d elapsed=%s",
				scanned, written.Load(), errors.Load(), time.Since(t0).Round(time.Second))
		}

		// Skip all-zero hash (rows indexed before creation bytecode tracking, or precompiles).
		if isZeroHash(creationHash) {
			continue
		}

		bucket := addrFirstByte(address)
		hashCopy := make([]byte, len(creationHash))
		copy(hashCopy, creationHash)

		ch <- work{
			hash:     hashCopy,
			seq:      creationSeq,
			bucket:   bucket,
			address:  address,
			blockNum: blockNum,
		}
	}
	close(ch)

	if err := iter.Close(); err != nil {
		log.Printf("[warn] iter close: %v", err)
	}

	// Wait for all writer goroutines to finish (channel already closed above).
	wg.Wait()
	close(done)

	elapsed := time.Since(t0).Round(time.Second)
	skipped := scanned - written.Load() - errors.Load()
	log.Printf("=== DONE ===")
	log.Printf("scanned=%d  written=%d  skipped(zero-hash)=%d  errors=%d  elapsed=%s",
		scanned, written.Load(), skipped, errors.Load(), elapsed)

	if errors.Load() > 0 {
		log.Printf("ERRORS DETECTED — re-run to retry failed rows")
	} else {
		log.Printf("OK — no errors")
	}
}

// isZeroHash returns true if b is nil, empty, or all-zero bytes.
func isZeroHash(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// addrFirstByte extracts the first byte of a hex Ethereum address (0x-prefixed or not).
// This matches the bucket scheme used by the indexer (addrFirstByte in bytecode_store.zig).
func addrFirstByte(addr string) int16 {
	s := strings.TrimPrefix(strings.TrimSpace(addr), "0x")
	s = strings.TrimPrefix(s, "0X")
	if len(s) < 2 {
		return 0
	}
	b, err := hex.DecodeString(s[:2])
	if err != nil || len(b) == 0 {
		return 0
	}
	return int16(b[0])
}
