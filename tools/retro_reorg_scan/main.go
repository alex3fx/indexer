// retro_reorg_scan: retrospective reorg detection for ERC-20 indexer.
//
// For each block in [from, to] where block_hash is not stored (pre-v22):
//   1. Fetches canonical tx hashes from Ethereum RPC.
//   2. Fetches stored tx hashes from eth.transactions in Scylla.
//   3. If the sets differ → we indexed an orphaned block → writes to eth.forked_blocks.
//
// tx_count comparison alone is insufficient: reorged blocks often have the same
// transaction count as the canonical block (different txs, same mempool at the same moment).
// Hash-set comparison is definitive.
//
// Usage:
//
//	./retro_reorg_scan \
//	  --from=25422404 --to=25580592 \
//	  --rpc=http://100.64.0.60:8545 \
//	  --pass=cassandra
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

const (
	chunkBuckets = int64(24)
	chunkEra     = int64(12000)
)

func blockToChunk(b int64) int32 {
	return int32((b%chunkBuckets) + chunkBuckets*(b/chunkEra))
}

// computeChunks returns all chunk values covering blocks in [from, to].
func computeChunks(from, to int64) []int32 {
	eraFrom := from / chunkEra
	eraTo := to / chunkEra
	chunks := make([]int32, 0, (eraTo-eraFrom+1)*chunkBuckets)
	for era := eraFrom; era <= eraTo; era++ {
		for b := int64(0); b < chunkBuckets; b++ {
			chunks = append(chunks, int32(b+chunkBuckets*era))
		}
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	return chunks
}

// ─── Scylla ──────────────────────────────────────────────────────────────────

type blockMeta struct {
	blockNumber int64
	txCount     int32
	logCount    int32
	itxCount    int32
	blockHash   string // "" means NULL (pre-v22)
}

// scanBlockCompletions returns all block_completions rows for [from, to].
func scanBlockCompletions(session *gocql.Session, chunks []int32, from, to int64) ([]blockMeta, error) {
	sem := make(chan struct{}, 32)
	var mu sync.Mutex
	var all []blockMeta
	var firstErr error
	var wg sync.WaitGroup

	for _, chunk := range chunks {
		chunk := chunk
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			var rows []blockMeta
			delay := 2 * time.Second
			for attempt := 1; attempt <= 5; attempt++ {
				rows = rows[:0]
				iter := session.Query(
					`SELECT block_number, tx_count, log_count, itx_count, block_hash
					 FROM block_completions
					 WHERE chunk=? AND block_number>=? AND block_number<=?`,
					chunk, from, to,
				).PageSize(5000).Iter()

				var (
					blockNumber               int64
					txCount, logCount, itxCount int32
					blockHash                 *string
				)
				for iter.Scan(&blockNumber, &txCount, &logCount, &itxCount, &blockHash) {
					bh := ""
					if blockHash != nil {
						bh = *blockHash
					}
					rows = append(rows, blockMeta{
						blockNumber: blockNumber,
						txCount:     txCount,
						logCount:    logCount,
						itxCount:    itxCount,
						blockHash:   bh,
					})
				}
				if err := iter.Close(); err != nil {
					log.Printf("[scylla] chunk %d attempt %d: %v", chunk, attempt, err)
					if attempt < 5 {
						time.Sleep(delay)
						delay *= 2
						continue
					}
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("chunk %d: %v", chunk, err)
					}
					mu.Unlock()
					return
				}
				break
			}

			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return all, firstErr
}

// storedTxHashes returns the set of tx hashes we stored for a given block.
func storedTxHashes(session *gocql.Session, blockNumber int64) (map[string]struct{}, error) {
	chunk := blockToChunk(blockNumber)
	delay := time.Second
	for attempt := 1; attempt <= 5; attempt++ {
		hashes := make(map[string]struct{})
		iter := session.Query(
			`SELECT hash FROM transactions WHERE chunk=? AND block_number=?`,
			chunk, blockNumber,
		).PageSize(2000).Iter()

		var h string
		for iter.Scan(&h) {
			hashes[h] = struct{}{}
		}
		if err := iter.Close(); err != nil {
			log.Printf("[scylla] txhashes block %d attempt %d: %v", blockNumber, attempt, err)
			if attempt < 5 {
				time.Sleep(delay)
				delay *= 2
				continue
			}
			return nil, fmt.Errorf("txhashes block %d: %v", blockNumber, err)
		}
		return hashes, nil
	}
	return nil, fmt.Errorf("txhashes block %d: all retries failed", blockNumber)
}

// ─── RPC ─────────────────────────────────────────────────────────────────────

type rpcReq struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type blockResp struct {
	Result *struct {
		Transactions []string `json:"transactions"` // false → hashes only
		Hash         string   `json:"hash"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type canonicalBlock struct {
	hash   string
	txSet  map[string]struct{}
	txList []string
}

func getCanonicalBlock(client *http.Client, rpcURL string, blockNumber int64) (*canonicalBlock, error) {
	body, _ := json.Marshal(rpcReq{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{fmt.Sprintf("0x%x", blockNumber), false},
	})

	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
		resp, err := client.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("[rpc] block %d attempt %d: %v", blockNumber, attempt+1, err)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		var result blockResp
		if err := json.Unmarshal(data, &result); err != nil {
			continue
		}
		if result.Error != nil {
			return nil, fmt.Errorf("RPC error: %s", result.Error.Message)
		}
		if result.Result == nil {
			return nil, fmt.Errorf("null block for %d", blockNumber)
		}
		txSet := make(map[string]struct{}, len(result.Result.Transactions))
		for _, h := range result.Result.Transactions {
			txSet[h] = struct{}{}
		}
		return &canonicalBlock{
			hash:   result.Result.Hash,
			txSet:  txSet,
			txList: result.Result.Transactions,
		}, nil
	}
	return nil, fmt.Errorf("all retries failed for block %d", blockNumber)
}

// ─── forked_blocks ───────────────────────────────────────────────────────────

type reorgEntry struct {
	blockNumber    int64
	storedTxCount  int32
	canonicalTx    int32
	canonicalHash  string
	orphanOnlyTxs  int // txs in our stored set but NOT in canonical
	lostTxs        int // txs in canonical but NOT in our stored set
	storedLog      int32
	storedItx      int32
}

func insertForkedBlock(session *gocql.Session, e reorgEntry) error {
	delay := time.Second
	for attempt := 1; attempt <= 5; attempt++ {
		err := session.Query(`
			INSERT INTO forked_blocks
			(block_number, block_hash, miner, block_timestamp, era, depth, reorg_group_id,
			 affected_txns_count_orphan, affected_txns_count_lost,
			 affected_logs_count_orphan, affected_logs_count_lost,
			 affected_traces_count_orphan, affected_traces_count_lost)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			e.blockNumber,
			"retro",
			"",
			int64(0),
			"pos",
			1,
			fmt.Sprintf("retro_scan_%d", e.blockNumber),
			int32(e.orphanOnlyTxs), // txs in our block that aren't canonical
			int32(e.lostTxs),       // txs in canonical we didn't index
			e.storedLog,
			int32(0),
			e.storedItx,
			int32(0),
		).Exec()
		if err == nil {
			return nil
		}
		log.Printf("[db] insert block %d attempt %d: %v", e.blockNumber, attempt, err)
		if attempt < 5 {
			time.Sleep(delay)
			delay *= 2
		}
	}
	return fmt.Errorf("insert block %d failed after retries", e.blockNumber)
}

// ─── worker ──────────────────────────────────────────────────────────────────

type workItem struct {
	meta blockMeta
}

type workResult struct {
	blockNumber   int64
	reorg         *reorgEntry
	err           error
	scyllaErr     bool
}

func checkBlock(session *gocql.Session, client *http.Client, rpcURL string, meta blockMeta) workResult {
	// Fetch canonical block from RPC
	cb, err := getCanonicalBlock(client, rpcURL, meta.blockNumber)
	if err != nil {
		return workResult{blockNumber: meta.blockNumber, err: err}
	}

	// Fetch stored tx hashes from Scylla
	stored, err := storedTxHashes(session, meta.blockNumber)
	if err != nil {
		return workResult{blockNumber: meta.blockNumber, err: err, scyllaErr: true}
	}

	// Quick path: if counts match exactly and sets are the same size, check hashes
	if int32(len(cb.txSet)) == meta.txCount && len(stored) == len(cb.txSet) {
		match := true
		for h := range stored {
			if _, ok := cb.txSet[h]; !ok {
				match = false
				break
			}
		}
		if match {
			return workResult{blockNumber: meta.blockNumber} // clean
		}
	}

	// Compute symmetric difference
	orphanOnly := 0
	for h := range stored {
		if _, ok := cb.txSet[h]; !ok {
			orphanOnly++
		}
	}
	lost := 0
	for h := range cb.txSet {
		if _, ok := stored[h]; !ok {
			lost++
		}
	}

	if orphanOnly == 0 && lost == 0 {
		return workResult{blockNumber: meta.blockNumber} // clean
	}

	return workResult{
		blockNumber: meta.blockNumber,
		reorg: &reorgEntry{
			blockNumber:   meta.blockNumber,
			storedTxCount: meta.txCount,
			canonicalTx:   int32(len(cb.txList)),
			canonicalHash: cb.hash,
			orphanOnlyTxs: orphanOnly,
			lostTxs:       lost,
			storedLog:     meta.logCount,
			storedItx:     meta.itxCount,
		},
	}
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	fromBlock := flag.Int64("from", 25422404, "start block (inclusive)")
	toBlock   := flag.Int64("to", 25580592, "end block (inclusive)")
	rpcURL    := flag.String("rpc", "http://100.64.0.60:8545", "Ethereum RPC URL")
	host      := flag.String("host", "127.0.0.1", "Scylla host")
	port      := flag.Int("port", 9042, "Scylla port")
	user      := flag.String("user", "cassandra", "Scylla user")
	pass      := flag.String("pass", "", "Scylla password")
	workers   := flag.Int("workers", 16, "parallel workers (each does 1 RPC + 1 Scylla per block)")
	dryRun    := flag.Bool("dry-run", false, "print mismatches only; don't write to forked_blocks")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	log.Printf("retro_reorg_scan v2 (tx-hash comparison): blocks [%d, %d]  rpc=%s  workers=%d  dry-run=%v",
		*fromBlock, *toBlock, *rpcURL, *workers, *dryRun)

	// ── Connect Scylla ──────────────────────────────────────────────────────
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = *workers + 4
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("scylla connect: %v", err)
	}
	defer session.Close()

	// ── Phase 1: scan block_completions ─────────────────────────────────────
	chunks := computeChunks(*fromBlock, *toBlock)
	log.Printf("Phase 1: scanning %d chunks in block_completions...", len(chunks))
	t0 := time.Now()
	rows, err := scanBlockCompletions(session, chunks, *fromBlock, *toBlock)
	if err != nil {
		log.Fatalf("scan failed: %v", err)
	}
	log.Printf("Phase 1 done: %d rows in %.1fs", len(rows), time.Since(t0).Seconds())

	// Keep only pre-v22 blocks (no block_hash stored)
	toCheck := rows[:0]
	var skipped int
	for _, r := range rows {
		if r.blockHash == "" {
			toCheck = append(toCheck, r)
		} else {
			skipped++
		}
	}
	log.Printf("Phase 2: %d blocks to check via tx-hash comparison (skipped %d with existing block_hash)",
		len(toCheck), skipped)

	if len(toCheck) == 0 {
		log.Println("Nothing to check.")
		return
	}

	// ── Phase 2: parallel tx-hash checks ────────────────────────────────────
	work    := make(chan blockMeta, 256)
	results := make(chan workResult, 256)

	client := &http.Client{Timeout: 30 * time.Second}

	var workerWg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for m := range work {
				results <- checkBlock(session, client, *rpcURL, m)
			}
		}()
	}
	go func() {
		for _, r := range toCheck {
			work <- r
		}
		close(work)
	}()
	go func() {
		workerWg.Wait()
		close(results)
	}()

	var checked, errCount, reorgCount int64
	var reorgs []reorgEntry

	t1 := time.Now()
	doneCh := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				log.Printf("[progress] checked=%d/%d  reorgs=%d  errors=%d  elapsed=%.0fs",
					atomic.LoadInt64(&checked), int64(len(toCheck)),
					atomic.LoadInt64(&reorgCount), atomic.LoadInt64(&errCount),
					time.Since(t1).Seconds())
			case <-doneCh:
				return
			}
		}
	}()

	for res := range results {
		if res.err != nil {
			atomic.AddInt64(&errCount, 1)
			log.Printf("[ERROR] block %d: %v", res.blockNumber, res.err)
			continue
		}
		atomic.AddInt64(&checked, 1)
		if res.reorg != nil {
			atomic.AddInt64(&reorgCount, 1)
			e := res.reorg
			reorgs = append(reorgs, *e)
			log.Printf("[REORG] block=%d  stored_tx=%d  canonical_tx=%d  orphan_only=%d  lost=%d  canonical_hash=%s",
				e.blockNumber, e.storedTxCount, e.canonicalTx,
				e.orphanOnlyTxs, e.lostTxs, e.canonicalHash)
		}
	}
	close(doneCh)

	log.Printf("Phase 2 done: checked=%d  reorgs=%d  errors=%d  in %.1fs",
		checked, reorgCount, errCount, time.Since(t1).Seconds())

	if len(reorgs) == 0 {
		log.Println("No reorgs detected — data is clean.")
		return
	}

	if *dryRun {
		log.Printf("[dry-run] %d reorg(s) — not writing to forked_blocks:", len(reorgs))
		for _, e := range reorgs {
			fmt.Printf("  block=%d  stored_tx=%d  canonical_tx=%d  orphan_only=%d  lost=%d\n",
				e.blockNumber, e.storedTxCount, e.canonicalTx, e.orphanOnlyTxs, e.lostTxs)
		}
		return
	}

	// ── Phase 3: write to forked_blocks ─────────────────────────────────────
	log.Printf("Phase 3: writing %d rows to eth.forked_blocks...", len(reorgs))
	var writeOK, writeErr int
	for _, e := range reorgs {
		if err := insertForkedBlock(session, e); err != nil {
			log.Printf("[ERROR] %v", err)
			writeErr++
		} else {
			writeOK++
		}
	}
	log.Printf("Phase 3 done: written=%d  errors=%d", writeOK, writeErr)
}
