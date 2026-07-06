// backfill_deployer_v3: fills deployer and tx_hash in contracts_by_address_v2
// using data already in the transactions table — no ETH node calls required.
//
// The transactions table stores every transaction ever indexed, including direct
// contract creations (where to_address IS NULL and contract_address IS NOT NULL).
// Those rows already have the deployer (from_address) and tx_hash (hash).
//
// Strategy:
//   - Scan all chunks [0, maxChunk] of eth.transactions.
//   - For each chunk: SELECT all rows, filter contract_address != "".
//   - For each match: UPDATE contracts_by_address_v2 SET deployer, tx_hash.
//   - This covers direct EOA → contract creates (no factory/CREATE2 needed).
//   - Factory creates (deployed by other contracts via CREATE/CREATE2) are NOT
//     in transactions.contract_address and still need trace_filter (v2 tool).
//
// Chunk formula (must match indexer config):
//   chunk = (block_number % SCYLLA_CHUNK_BUCKETS) +
//           SCYLLA_CHUNK_BUCKETS * floor(block_number / SCYLLA_CHUNK_ERA)
//
// Usage:
//
//	./backfill_deployer_v3 --host=127.0.0.1 --pass=cassandra \
//	    [--from-chunk=0] [--to-chunk=52000] \
//	    [--workers=200] [--db-workers=512] [--log-every=1000]
package main

import (
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

var (
	session *gocql.Session

	chunksTotal  int64
	chunksDone   atomic.Int64
	txsScanned   atomic.Int64
	directFound  atomic.Int64
	updated      atomic.Int64
	dbErrors     atomic.Int64
)

const (
	selTxsQ  = `SELECT block_number, contract_address, from_address, hash FROM eth.transactions WHERE chunk = ?`
	updDeployQ = `UPDATE eth.contracts_by_address_v2 SET deployer = ?, tx_hash = ? WHERE address = ? AND block_number = ?`
)

type deployWork struct {
	blockNum    int64
	address     string
	fromAddress string
	hash        string
}

func main() {
	host      := flag.String("host", "127.0.0.1", "Scylla host")
	port      := flag.Int("port", 9042, "Scylla port")
	user      := flag.String("user", "cassandra", "Scylla user")
	pass      := flag.String("pass", "", "Scylla password (required)")
	fromChunk := flag.Int("from-chunk", 0, "first chunk to scan (inclusive)")
	toChunk   := flag.Int("to-chunk", 52000, "last chunk to scan (inclusive)")
	workers   := flag.Int("workers", 200, "concurrent chunk-scan goroutines")
	dbWkrs    := flag.Int("db-workers", 512, "concurrent Scylla UPDATE goroutines")
	logEvery  := flag.Int("log-every", 1000, "log progress every N chunks")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass is required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 32
	cluster.Timeout = 120 * time.Second
	cluster.PageSize = 5000

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect scylla: %v", err)
	}
	defer session.Close()

	chunksTotal = int64(*toChunk - *fromChunk + 1)
	log.Printf("=== DEPLOYER BACKFILL v3 (DB-only, direct creates) ===")
	log.Printf("chunks=%d→%d total=%d workers=%d db_workers=%d",
		*fromChunk, *toChunk, chunksTotal, *workers, *dbWkrs)

	start := time.Now()

	chunkCh  := make(chan int, *workers*4)
	updateCh := make(chan deployWork, *dbWkrs*4)

	// DB writer pool.
	var writerWg sync.WaitGroup
	for i := 0; i < *dbWkrs; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for w := range updateCh {
				doUpdate(w)
			}
		}()
	}

	// Chunk scanner pool.
	var scanWg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		scanWg.Add(1)
		go func() {
			defer scanWg.Done()
			for chunk := range chunkCh {
				scanChunk(chunk, updateCh, *logEvery)
			}
		}()
	}

	// Progress reporter.
	stopProgress := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopProgress:
				return
			case <-time.After(30 * time.Second):
				done := chunksDone.Load()
				total := chunksTotal
				pct := float64(done) / float64(total) * 100
				elapsed := time.Since(start)
				var eta string
				if done > 0 {
					rem := time.Duration(float64(elapsed) / float64(done) * float64(total-done))
					eta = rem.Round(time.Second).String()
				} else {
					eta = "?"
				}
				log.Printf("[progress] chunks=%d/%d (%.1f%%) txs=%d creates=%d updated=%d dbErr=%d eta=%s",
					done, total, pct,
					txsScanned.Load(), directFound.Load(), updated.Load(),
					dbErrors.Load(), eta)
			}
		}
	}()

	for chunk := *fromChunk; chunk <= *toChunk; chunk++ {
		chunkCh <- chunk
	}
	close(chunkCh)

	scanWg.Wait()
	close(updateCh)
	writerWg.Wait()
	close(stopProgress)

	elapsed := time.Since(start).Round(time.Second)
	log.Printf("=== DONE === chunks=%d txs=%d creates=%d updated=%d dbErr=%d elapsed=%s",
		chunksDone.Load(), txsScanned.Load(), directFound.Load(), updated.Load(),
		dbErrors.Load(), elapsed)

	if dbErrors.Load() > 0 {
		log.Printf("DB ERRORS DETECTED — re-run to retry (idempotent)")
	} else {
		log.Printf("OK — no errors")
	}
}

func scanChunk(chunk int, updateCh chan<- deployWork, logEvery int) {
	defer chunksDone.Add(1)

	iter := session.Query(selTxsQ, chunk).Iter()
	var blockNum int64
	var contractAddr, fromAddr, hash string
	local := int64(0)
	creates := int64(0)
	for iter.Scan(&blockNum, &contractAddr, &fromAddr, &hash) {
		local++
		if contractAddr != "" {
			creates++
			updateCh <- deployWork{
				blockNum:    blockNum,
				address:     contractAddr,
				fromAddress: fromAddr,
				hash:        hash,
			}
		}
	}
	if err := iter.Close(); err != nil {
		dbErrors.Add(1)
		log.Printf("[scan-err] chunk=%d: %v", chunk, err)
	}
	txsScanned.Add(local)
	directFound.Add(creates)

	done := chunksDone.Load() + 1
	if logEvery > 0 && int(done)%logEvery == 0 {
		log.Printf("[milestone] chunk=%d done=%d creates_so_far=%d", chunk, done, directFound.Load())
	}
}

func doUpdate(w deployWork) {
	err := withRetry(func() error {
		return session.Query(updDeployQ, w.fromAddress, w.hash, w.address, w.blockNum).Exec()
	})
	if err != nil {
		dbErrors.Add(1)
		log.Printf("[db-err] UPDATE addr=%s block=%d: %v", w.address, w.blockNum, err)
		return
	}
	updated.Add(1)
}

func withRetry(f func() error) error {
	for attempt := 0; attempt < 5; attempt++ {
		if err := f(); err != nil {
			if attempt == 4 {
				return err
			}
			time.Sleep(time.Duration(1<<uint(attempt)) * 500 * time.Millisecond)
			continue
		}
		return nil
	}
	return nil
}
