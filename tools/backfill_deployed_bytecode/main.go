// backfill_deployed_bytecode: fills bytecode_hash / bytecode_seq for historical rows in
// contracts_by_address_v2 where these fields are NULL (created by merge_old_to_v2 UPDATE).
//
// For each such row:
//   1. Call eth_getCode(address, block_number) in JSON-RPC batches.
//   2. If non-empty: SHA256 → INSERT bytecode_store_v2 IF NOT EXISTS → UPDATE contracts_by_address_v2 → INSERT addresses_by_bytecode.
//   3. Skip rows with already-populated bytecode_hash (idempotent full-scan).
//
// Safe to re-run: INSERT addresses_by_bytecode is upsert; UPDATE contracts is idempotent.
//
// Usage:
//
//	./backfill_deployed_bytecode --rpc=http://100.64.0.60:8545 --host=127.0.0.1 --pass=cassandra \
//	    [--rpc-workers=40] [--rpc-batch=100] [--db-workers=256] [--page-size=5000] [--log-every=100000]
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

// ── RPC types ────────────────────────────────────────────────────────────────

type rpcReq struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type rpcResp struct {
	ID     int    `json:"id"`
	Result string `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// ── work item ────────────────────────────────────────────────────────────────

type job struct {
	address  string
	blockNum int64
}

// ── globals ──────────────────────────────────────────────────────────────────

var (
	rpcURL  string
	session *gocql.Session

	scanned   atomic.Int64
	processed atomic.Int64 // rows with non-empty bytecode written
	skipped   atomic.Int64 // already had bytecode_hash OR empty bytecode
	rpcErrors atomic.Int64
	dbErrors  atomic.Int64
)

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	host       := flag.String("host", "127.0.0.1", "Scylla host")
	port       := flag.Int("port", 9042, "Scylla port")
	user       := flag.String("user", "cassandra", "Scylla user")
	pass       := flag.String("pass", "", "Scylla password (required)")
	keyspace   := flag.String("keyspace", "eth", "Scylla keyspace")
	rpc        := flag.String("rpc", "", "ETH JSON-RPC URL (required)")
	rpcWorkers := flag.Int("rpc-workers", 40, "concurrent RPC batch goroutines")
	rpcBatch   := flag.Int("rpc-batch", 100, "eth_getCode calls per RPC batch")
	dbWorkers  := flag.Int("db-workers", 256, "concurrent Scylla write goroutines")
	pageSize   := flag.Int("page-size", 5000, "CQL fetch page size")
	logEvery   := flag.Int64("log-every", 100_000, "log progress every N scanned rows")
	flag.Parse()

	if *pass == "" { log.Fatal("--pass is required") }
	if *rpc == ""  { log.Fatal("--rpc is required") }
	rpcURL = *rpc

	// ── Scylla session ─────────────────────────────────────────────────────
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 16
	cluster.PageSize = *pageSize
	cluster.Timeout = 120 * time.Second

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// ── channels ───────────────────────────────────────────────────────────
	jobCh  := make(chan job, *rpcWorkers**rpcBatch*2)
	writeCh := make(chan writeWork, *dbWorkers*4)

	// ── DB writer goroutines ───────────────────────────────────────────────
	var writerWg sync.WaitGroup
	for i := 0; i < *dbWorkers; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for w := range writeCh {
				doWrite(w)
			}
		}()
	}

	// ── RPC worker goroutines ──────────────────────────────────────────────
	var rpcWg sync.WaitGroup
	for i := 0; i < *rpcWorkers; i++ {
		rpcWg.Add(1)
		go func() {
			defer rpcWg.Done()
			buf := make([]job, 0, *rpcBatch)
			for j := range jobCh {
				buf = append(buf, j)
				if len(buf) == *rpcBatch {
					processBatch(buf, writeCh)
					buf = buf[:0]
				}
			}
			if len(buf) > 0 {
				processBatch(buf, writeCh)
			}
		}()
	}

	// ── Progress reporter ─────────────────────────────────────────────────
	stopProgress := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopProgress:
				return
			case <-time.After(30 * time.Second):
				log.Printf("[progress] scanned=%d processed=%d skipped=%d rpcErr=%d dbErr=%d",
					scanned.Load(), processed.Load(), skipped.Load(), rpcErrors.Load(), dbErrors.Load())
			}
		}
	}()

	// ── Full table scan ───────────────────────────────────────────────────
	log.Printf("Starting full scan of contracts_by_address_v2 (page=%d rpc_workers=%d rpc_batch=%d db_workers=%d)",
		*pageSize, *rpcWorkers, *rpcBatch, *dbWorkers)
	t0 := time.Now()

	iter := session.Query(
		`SELECT address, block_number, bytecode_hash FROM contracts_by_address_v2`,
	).PageSize(*pageSize).Iter()

	var address string
	var blockNum int64
	var bytecodeHash []byte

	for iter.Scan(&address, &blockNum, &bytecodeHash) {
		n := scanned.Add(1)
		if n%*logEvery == 0 {
			log.Printf("[scan] scanned=%d processed=%d skipped=%d rpcErr=%d dbErr=%d elapsed=%s",
				n, processed.Load(), skipped.Load(), rpcErrors.Load(), dbErrors.Load(),
				time.Since(t0).Round(time.Second))
		}

		// Skip rows that already have bytecode_hash populated.
		if len(bytecodeHash) > 0 {
			skipped.Add(1)
			continue
		}

		jobCh <- job{address: address, blockNum: blockNum}
	}
	close(jobCh)

	if err := iter.Close(); err != nil {
		log.Printf("[warn] scan iter close: %v", err)
	}

	rpcWg.Wait()
	close(writeCh)
	writerWg.Wait()
	close(stopProgress)

	elapsed := time.Since(t0).Round(time.Second)
	log.Printf("=== DONE === scanned=%d processed=%d skipped=%d rpcErr=%d dbErr=%d elapsed=%s",
		scanned.Load(), processed.Load(), skipped.Load(), rpcErrors.Load(), dbErrors.Load(), elapsed)

	if rpcErrors.Load() > 0 || dbErrors.Load() > 0 {
		log.Printf("ERRORS DETECTED — re-run to retry failed rows (idempotent)")
	} else {
		log.Printf("OK — no errors")
	}
}

// ── RPC batch ────────────────────────────────────────────────────────────────

func processBatch(jobs []job, writeCh chan<- writeWork) {
	reqs := make([]rpcReq, len(jobs))
	for i, j := range jobs {
		reqs[i] = rpcReq{
			JSONRPC: "2.0",
			Method:  "eth_getCode",
			Params:  []interface{}{j.address, fmt.Sprintf("0x%x", j.blockNum)},
			ID:      i,
		}
	}

	body, _ := json.Marshal(reqs)

	var resps []rpcResp
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			rpcErrors.Add(int64(len(jobs)))
			log.Printf("[rpc-err] batch len=%d attempt=%d: %v", len(jobs), attempt+1, err)
			if attempt == 3 {
				return
			}
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err := json.Unmarshal(respBody, &resps); err != nil {
			rpcErrors.Add(int64(len(jobs)))
			log.Printf("[rpc-err] decode batch len=%d attempt=%d: %v", len(jobs), attempt+1, err)
			if attempt == 3 {
				return
			}
			continue
		}
		break
	}

	// Build id→result map (responses may come out of order).
	resultByID := make(map[int]string, len(resps))
	for _, r := range resps {
		if r.Error != nil {
			rpcErrors.Add(1)
			log.Printf("[rpc-err] id=%d: %s", r.ID, r.Error.Message)
			continue
		}
		resultByID[r.ID] = r.Result
	}

	for i, j := range jobs {
		hexCode, ok := resultByID[i]
		if !ok {
			continue
		}
		// Empty bytecode: 0x or 0x<nothing>.
		clean := strings.TrimPrefix(hexCode, "0x")
		clean = strings.TrimPrefix(clean, "0X")
		if clean == "" {
			skipped.Add(1)
			continue
		}
		raw, err := hex.DecodeString(clean)
		if err != nil || len(raw) == 0 {
			skipped.Add(1)
			continue
		}

		hash := sha256.Sum256(raw)
		hashSlice := hash[:]

		writeCh <- writeWork{
			address:  j.address,
			blockNum: j.blockNum,
			hash:     hashSlice,
			bytecode: raw,
		}
	}
}

// ── DB write ──────────────────────────────────────────────────────────────────

type writeWork struct {
	address  string
	blockNum int64
	hash     []byte
	bytecode []byte
}

const (
	insStoreQ = `INSERT INTO bytecode_store_v2 (hash, seq, kind, bytecode, first_seen_block, size) VALUES (?, ?, 0, ?, ?, ?) IF NOT EXISTS`
	updContQ  = `UPDATE contracts_by_address_v2 SET bytecode_hash = ?, bytecode_seq = ? WHERE address = ? AND block_number = ?`
	insAddrQ  = `INSERT INTO addresses_by_bytecode (hash, seq, bucket, address, block_number) VALUES (?, ?, ?, ?, ?)`
)

func doWrite(w writeWork) {
	const seq int8 = 0 // SHA256 collisions don't occur in practice
	bucket := addrFirstByte(w.address)
	size := int32(len(w.bytecode))

	// 1. INSERT bytecode_store_v2 IF NOT EXISTS — idempotent first-writer-wins.
	if err := withRetry(func() error {
		return session.Query(insStoreQ, w.hash, seq, w.bytecode, w.blockNum, size).Exec()
	}); err != nil {
		dbErrors.Add(1)
		log.Printf("[db-err] insStore addr=%s block=%d: %v", w.address, w.blockNum, err)
		return
	}

	// 2. UPDATE contracts_by_address_v2 with hash.
	if err := withRetry(func() error {
		return session.Query(updContQ, w.hash, seq, w.address, w.blockNum).Exec()
	}); err != nil {
		dbErrors.Add(1)
		log.Printf("[db-err] updCont addr=%s block=%d: %v", w.address, w.blockNum, err)
		return
	}

	// 3. INSERT addresses_by_bytecode (reverse index).
	if err := withRetry(func() error {
		return session.Query(insAddrQ, w.hash, seq, bucket, w.address, w.blockNum).Exec()
	}); err != nil {
		dbErrors.Add(1)
		log.Printf("[db-err] insAddr addr=%s block=%d: %v", w.address, w.blockNum, err)
		return
	}

	processed.Add(1)
}

// withRetry retries f up to 5 times with exponential backoff.
func withRetry(f func() error) error {
	for attempt := 0; attempt < 5; attempt++ {
		if err := f(); err != nil {
			if attempt == 4 {
				return err
			}
			time.Sleep(time.Duration(1<<attempt) * 500 * time.Millisecond)
			continue
		}
		return nil
	}
	return nil
}

// addrFirstByte extracts the first byte of a hex Ethereum address (0x-prefixed or bare).
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
