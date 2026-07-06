// backfill_deployer: fills deployer and tx_hash in contracts_by_address_v2 for historical rows
// where these fields are NULL (created by merge_old_to_v2 UPDATE-only backfill).
//
// Strategy (two phases):
//
//   Phase 1 — Scylla scan:
//     Full scan of contracts_by_address_v2, collect set of unique block_number values
//     where deployer IS NULL. Only block numbers are stored in memory (~8 bytes each),
//     NOT full addresses — keeps memory usage under ~80MB even for 10M unique blocks.
//
//   Phase 2 — trace_block processing:
//     For each unique block number (in parallel), call trace_block(blockNum).
//     Filter traces for type="create". For each create trace:
//       UPDATE contracts_by_address_v2 SET deployer=?, tx_hash=? WHERE address=? AND block_number=?
//     This is safe to over-apply (idempotent): rows that already have deployer are just re-written
//     with the same correct value. No need to check null first.
//
//   Key optimization: only blocks that appear in our null-deployer set are trace_block-ed.
//   If there are 10M unique blocks (out of 25M total), we only call trace_block 10M times,
//   not 25M. Most of those calls will return 0 creates (they had no contracts), but the
//   block set ensures we don't miss any.
//
// Safe to re-run: UPDATE is idempotent. Phase 1 re-collects from current state.
//
// Usage:
//
//	./backfill_deployer --rpc=http://100.64.0.60:8545 --host=127.0.0.1 --pass=cassandra \
//	    [--trace-workers=100] [--db-workers=256] [--page-size=5000] [--log-every=500000]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

// ── RPC types ─────────────────────────────────────────────────────────────────

type traceAction struct {
	From string `json:"from"`
	Init string `json:"init"` // creation bytecode (not needed, but present)
}

type traceResult struct {
	Address string `json:"address"` // deployed contract address
}

type traceEntry struct {
	Type            string      `json:"type"`            // "create", "call", "suicide", etc.
	Action          traceAction `json:"action"`
	Result          traceResult `json:"result"`
	TransactionHash string      `json:"transactionHash"`
	BlockNumber     int64       `json:"blockNumber"`
}

// ── globals ───────────────────────────────────────────────────────────────────

var (
	rpcURL  string
	session *gocql.Session

	// Phase 1 counters.
	p1Scanned    atomic.Int64
	p1NullRows   atomic.Int64
	// Phase 2 counters.
	p2Blocks     atomic.Int64
	p2Updated    atomic.Int64
	p2ZeroCreate atomic.Int64
	p2RpcErrors  atomic.Int64
	p2DbErrors   atomic.Int64
)

const updDeployerQ = `UPDATE contracts_by_address_v2 SET deployer = ?, tx_hash = ? WHERE address = ? AND block_number = ?`

func main() {
	host         := flag.String("host", "127.0.0.1", "Scylla host")
	port         := flag.Int("port", 9042, "Scylla port")
	user         := flag.String("user", "cassandra", "Scylla user")
	pass         := flag.String("pass", "", "Scylla password (required)")
	keyspace     := flag.String("keyspace", "eth", "Scylla keyspace")
	rpc          := flag.String("rpc", "", "ETH JSON-RPC URL (required)")
	traceWorkers := flag.Int("trace-workers", 100, "concurrent trace_block goroutines")
	dbWorkers    := flag.Int("db-workers", 256, "concurrent Scylla UPDATE goroutines")
	pageSize     := flag.Int("page-size", 5000, "CQL fetch page size")
	logEvery     := flag.Int64("log-every", 500_000, "log progress every N rows (phase 1)")
	phase1Only   := flag.Bool("phase1-only", false, "stop after phase 1 (print block count, no trace_block calls)")
	flag.Parse()

	if *pass == "" { log.Fatal("--pass is required") }
	if *rpc == ""  { log.Fatal("--rpc is required") }
	rpcURL = *rpc

	// ── Scylla session ────────────────────────────────────────────────────
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

	totalStart := time.Now()

	// ──────────────────────────────────────────────────────────────────────
	// PHASE 1: Collect unique block numbers where deployer IS NULL.
	// ──────────────────────────────────────────────────────────────────────
	log.Printf("=== PHASE 1: Scanning contracts_by_address_v2 for null-deployer blocks ===")
	log.Printf("(page_size=%d)", *pageSize)
	p1Start := time.Now()

	blockSet := make(map[int64]struct{}, 10_000_000)
	var blockSetMu sync.Mutex

	iter := session.Query(
		`SELECT block_number, deployer FROM contracts_by_address_v2`,
	).PageSize(*pageSize).Iter()

	var blockNum int64
	var deployer string

	for iter.Scan(&blockNum, &deployer) {
		n := p1Scanned.Add(1)
		if n%*logEvery == 0 {
			blockSetMu.Lock()
			numBlocks := len(blockSet)
			blockSetMu.Unlock()
			log.Printf("[phase1] scanned=%d null_rows=%d unique_blocks=%d elapsed=%s",
				n, p1NullRows.Load(), numBlocks, time.Since(p1Start).Round(time.Second))
		}
		if deployer != "" {
			continue // already populated
		}
		p1NullRows.Add(1)
		blockSetMu.Lock()
		blockSet[blockNum] = struct{}{}
		blockSetMu.Unlock()
	}
	if err := iter.Close(); err != nil {
		log.Printf("[phase1 warn] iter close: %v", err)
	}

	// Sort block numbers for deterministic processing order and progress estimation.
	blocks := make([]int64, 0, len(blockSet))
	for b := range blockSet {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	blockSet = nil // free memory

	log.Printf("=== PHASE 1 DONE: scanned=%d null_rows=%d unique_blocks=%d elapsed=%s ===",
		p1Scanned.Load(), p1NullRows.Load(), len(blocks),
		time.Since(p1Start).Round(time.Second))

	if len(blocks) == 0 {
		log.Printf("No null-deployer blocks found — nothing to do.")
		return
	}

	if *phase1Only {
		log.Printf("--phase1-only: stopping here. unique_blocks=%d", len(blocks))
		if len(blocks) > 0 {
			log.Printf("  min_block=%d max_block=%d", blocks[0], blocks[len(blocks)-1])
		}
		return
	}

	// ──────────────────────────────────────────────────────────────────────
	// PHASE 2: trace_block for each unique block, update deployer/tx_hash.
	// ──────────────────────────────────────────────────────────────────────
	log.Printf("=== PHASE 2: trace_block + UPDATE for %d unique blocks ===", len(blocks))
	log.Printf("(trace_workers=%d db_workers=%d)", *traceWorkers, *dbWorkers)
	p2Start := time.Now()

	blockCh := make(chan int64, *traceWorkers*4)
	updateCh := make(chan updateWork, *dbWorkers*4)

	// DB writer goroutines.
	var writerWg sync.WaitGroup
	for i := 0; i < *dbWorkers; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for w := range updateCh {
				doUpdate(w)
			}
		}()
	}

	// trace_block worker goroutines.
	var traceWg sync.WaitGroup
	for i := 0; i < *traceWorkers; i++ {
		traceWg.Add(1)
		go func() {
			defer traceWg.Done()
			for b := range blockCh {
				processBlock(b, updateCh)
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
				done := p2Blocks.Load()
				total := int64(len(blocks))
				pct := float64(done) / float64(total) * 100
				elapsed := time.Since(p2Start)
				var eta string
				if done > 0 {
					remaining := time.Duration(float64(elapsed) / float64(done) * float64(total-done))
					eta = remaining.Round(time.Second).String()
				} else {
					eta = "?"
				}
				log.Printf("[phase2] blocks=%d/%d (%.1f%%) updated=%d zero_create=%d rpcErr=%d dbErr=%d eta=%s",
					done, total, pct, p2Updated.Load(), p2ZeroCreate.Load(),
					p2RpcErrors.Load(), p2DbErrors.Load(), eta)
			}
		}
	}()

	// Feed blocks to workers.
	for _, b := range blocks {
		blockCh <- b
	}
	close(blockCh)

	traceWg.Wait()
	close(updateCh)
	writerWg.Wait()
	close(stopProgress)

	log.Printf("=== PHASE 2 DONE: blocks=%d updated=%d zero_create=%d rpcErr=%d dbErr=%d elapsed=%s ===",
		p2Blocks.Load(), p2Updated.Load(), p2ZeroCreate.Load(),
		p2RpcErrors.Load(), p2DbErrors.Load(),
		time.Since(p2Start).Round(time.Second))

	log.Printf("=== TOTAL elapsed=%s ===", time.Since(totalStart).Round(time.Second))

	if p2RpcErrors.Load() > 0 || p2DbErrors.Load() > 0 {
		log.Printf("ERRORS DETECTED — re-run to retry (idempotent)")
	} else {
		log.Printf("OK — no errors")
	}
}

// ── trace_block ───────────────────────────────────────────────────────────────

func processBlock(blockNum int64, updateCh chan<- updateWork) {
	p2Blocks.Add(1)

	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "trace_block",
		"params":  []interface{}{fmt.Sprintf("0x%x", blockNum)},
		"id":      1,
	}
	body, _ := json.Marshal(req)

	var respBody []byte
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			if attempt == 3 {
				p2RpcErrors.Add(1)
				log.Printf("[rpc-err] trace_block block=%d: %v", blockNum, err)
				return
			}
			continue
		}
		respBody, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		break
	}

	var result struct {
		Result []traceEntry `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		p2RpcErrors.Add(1)
		log.Printf("[rpc-err] decode block=%d: %v", blockNum, err)
		return
	}
	if result.Error != nil {
		p2RpcErrors.Add(1)
		log.Printf("[rpc-err] block=%d: %s", blockNum, result.Error.Message)
		return
	}

	creates := 0
	for _, t := range result.Result {
		if t.Type != "create" {
			continue
		}
		addr := t.Result.Address
		deployer := t.Action.From
		txHash := t.TransactionHash
		if addr == "" || deployer == "" {
			continue
		}
		creates++
		updateCh <- updateWork{
			address:  addr,
			blockNum: blockNum,
			deployer: deployer,
			txHash:   txHash,
		}
	}
	if creates == 0 {
		p2ZeroCreate.Add(1)
	}
}

// ── DB update ─────────────────────────────────────────────────────────────────

type updateWork struct {
	address  string
	blockNum int64
	deployer string
	txHash   string
}

func doUpdate(w updateWork) {
	err := withRetry(func() error {
		return session.Query(updDeployerQ, w.deployer, w.txHash, w.address, w.blockNum).Exec()
	})
	if err != nil {
		p2DbErrors.Add(1)
		log.Printf("[db-err] UPDATE addr=%s block=%d: %v", w.address, w.blockNum, err)
		return
	}
	p2Updated.Add(1)
}

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
