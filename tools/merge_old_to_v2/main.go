// merge_old_to_v2 — одноразовый backfill старых таблиц в v2-таблицы.
//
// Stream A: contracts → contracts_by_address_v2
//   Заполняет 6 новых полей (contract_factory, block_timestamp_s, block_timestamp_ms,
//   creation_method, transaction_index, trace_index) для блоков < WATERMARK.
//
// Stream B: bytecode_store → bytecode_store_v2
//   Заполняет first_seen_block для seq=0 строк.
//
// Запуск: merge_old_to_v2 --host 100.64.0.4 --user cassandra --pass cassandra [--stream a|b|both]
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

// Watermark: блоки начиная с этого номера индексировались v2-индексером (v13+).
// Для блоков ниже watermark данные есть только в старых таблицах.
const backfillWatermark = int64(25422404)

// Число воркеров для параллельных UPDATE
const numWorkers = 32

func main() {
	host := flag.String("host", "100.64.0.4", "Scylla host")
	port := flag.Int("port", 9042, "Scylla port")
	user := flag.String("user", "cassandra", "Scylla username")
	pass := flag.String("pass", "", "Scylla password")
	keyspace := flag.String("keyspace", "eth", "Scylla keyspace")
	stream := flag.String("stream", "both", "Which stream to run: a, b, or both")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass is required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *user,
		Password: *pass,
	}
	cluster.Consistency = gocql.LocalQuorum
	cluster.NumConns = 8
	cluster.Timeout = 60 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	// Не используем RetryPolicy на уровне кластера — retry реализован вручную.

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	switch *stream {
	case "a":
		streamA(session)
	case "b":
		streamB(session)
	case "both":
		streamA(session)
		streamB(session)
	default:
		log.Fatalf("unknown stream %q; use a, b, or both", *stream)
	}
}

// ─── Stream A ─────────────────────────────────────────────────────────────────

type contractsRow struct {
	address        string
	blockNumber    int64
	transactionIdx int
	traceIdx       int
	contractFactory string
	tsS            int64
	tsMs           int64
	creationMethod int8
}

func streamA(session *gocql.Session) {
	fmt.Println("=== Stream A: contracts → contracts_by_address_v2 ===")
	start := time.Now()

	var total, updated, skipped, errors int64

	jobs := make(chan contractsRow, numWorkers*4)
	var wg sync.WaitGroup

	// Workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range jobs {
				if err := updateContractV2(session, row); err != nil {
					atomic.AddInt64(&errors, 1)
					log.Printf("[A] update failed addr=%s blk=%d: %v", row.address, row.blockNumber, err)
				} else {
					atomic.AddInt64(&updated, 1)
				}
			}
		}()
	}

	// Scanner
	const sel = `SELECT chunk, block_number, transaction_index, trace_index, address,
		contract_factory, block_timestamp_s, block_timestamp_ms, creation_method
		FROM eth.contracts`

	iter := session.Query(sel).PageSize(5000).Iter()
	var (
		chunk           int
		blockNumber     int64
		transactionIdx  int
		traceIdx        int
		address         string
		contractFactory string
		tsS             int64
		tsMs            int64
		creationMethod  int8
	)
	_ = chunk

	for iter.Scan(&chunk, &blockNumber, &transactionIdx, &traceIdx, &address,
		&contractFactory, &tsS, &tsMs, &creationMethod) {

		n := atomic.AddInt64(&total, 1)
		if n%50000 == 0 {
			fmt.Printf("[A] scanned=%d updated=%d skipped=%d errors=%d elapsed=%s\n",
				n, atomic.LoadInt64(&updated), atomic.LoadInt64(&skipped), atomic.LoadInt64(&errors), time.Since(start).Round(time.Second))
		}

		// Фильтр: только блоки ниже watermark (выше обработает v16)
		if blockNumber >= backfillWatermark {
			atomic.AddInt64(&skipped, 1)
			continue
		}

		jobs <- contractsRow{
			address:         address,
			blockNumber:     blockNumber,
			transactionIdx:  transactionIdx,
			traceIdx:        traceIdx,
			contractFactory: contractFactory,
			tsS:             tsS,
			tsMs:            tsMs,
			creationMethod:  creationMethod,
		}
	}
	if err := iter.Close(); err != nil {
		log.Printf("[A] scan error: %v", err)
	}

	close(jobs)
	wg.Wait()

	fmt.Printf("[A] DONE scanned=%d updated=%d skipped=%d errors=%d elapsed=%s\n",
		total, updated, skipped, errors, time.Since(start).Round(time.Second))
}

func updateContractV2(session *gocql.Session, row contractsRow) error {
	const upd = `UPDATE eth.contracts_by_address_v2
		SET contract_factory = ?,
		    block_timestamp_s = ?,
		    block_timestamp_ms = ?,
		    creation_method = ?,
		    transaction_index = ?,
		    trace_index = ?
		WHERE address = ? AND block_number = ?`

	return retryQuery(func() error {
		return session.Query(upd,
			row.contractFactory,
			row.tsS,
			row.tsMs,
			row.creationMethod,
			row.transactionIdx,
			row.traceIdx,
			row.address,
			row.blockNumber,
		).Exec()
	})
}

// ─── Stream B ─────────────────────────────────────────────────────────────────

type bytecodeRow struct {
	hash           []byte
	firstSeenBlock int64
}

func streamB(session *gocql.Session) {
	fmt.Println("=== Stream B: bytecode_store → bytecode_store_v2 ===")
	start := time.Now()

	var total, updated, errors int64

	jobs := make(chan bytecodeRow, numWorkers*4)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range jobs {
				if err := updateBytecodeV2(session, row); err != nil {
					atomic.AddInt64(&errors, 1)
					log.Printf("[B] update failed hash=%x: %v", row.hash, err)
				} else {
					atomic.AddInt64(&updated, 1)
				}
			}
		}()
	}

	const sel = `SELECT bytecode_hash, first_seen_block FROM eth.bytecode_store`

	iter := session.Query(sel).PageSize(5000).Iter()
	var (
		hash           []byte
		firstSeenBlock int64
	)

	for iter.Scan(&hash, &firstSeenBlock) {
		n := atomic.AddInt64(&total, 1)
		if n%10000 == 0 {
			fmt.Printf("[B] scanned=%d updated=%d errors=%d elapsed=%s\n",
				n, atomic.LoadInt64(&updated), atomic.LoadInt64(&errors), time.Since(start).Round(time.Second))
		}

		hashCopy := make([]byte, len(hash))
		copy(hashCopy, hash)
		jobs <- bytecodeRow{hash: hashCopy, firstSeenBlock: firstSeenBlock}
	}
	if err := iter.Close(); err != nil {
		log.Printf("[B] scan error: %v", err)
	}

	close(jobs)
	wg.Wait()

	fmt.Printf("[B] DONE scanned=%d updated=%d errors=%d elapsed=%s\n",
		total, updated, errors, time.Since(start).Round(time.Second))
}

func updateBytecodeV2(session *gocql.Session, row bytecodeRow) error {
	const upd = `UPDATE eth.bytecode_store_v2 SET first_seen_block = ? WHERE hash = ? AND seq = 0`

	return retryQuery(func() error {
		return session.Query(upd, row.firstSeenBlock, row.hash).Exec()
	})
}

// ─── retry ────────────────────────────────────────────────────────────────────

func retryQuery(fn func() error) error {
	const maxRetries = 8
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if attempt == maxRetries-1 {
			return fmt.Errorf("after %d retries: %w", maxRetries, err)
		}
		jitter := time.Duration(float64(delay) * (0.8 + 0.4*math.Sin(float64(attempt))))
		if jitter < 100*time.Millisecond {
			jitter = 100 * time.Millisecond
		}
		time.Sleep(jitter)
		delay = min(delay*2, 30*time.Second)
	}
	return nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
