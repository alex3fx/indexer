// forked_block_checker: для заданного списка block numbers (из Etherscan forked list)
// сравнивает хранимые tx хэши в Scylla с canonical хэшами из RPC.
// Детальный отчёт: orphan_only (в БД, но не в каноникале) и lost (в каноникале, нет в БД).
// Сохраняет полный лог в файл, включая конкретные tx хэши.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
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

// ── RPC ──────────────────────────────────────────────────────────────────────

type rpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type blockResult struct {
	Transactions []string `json:"transactions"`
	Hash         string   `json:"hash"`
	Number       string   `json:"number"`
}

type blockResponse struct {
	Result *blockResult `json:"result"`
}

func getCanonicalBlock(client *http.Client, rpcURL string, blockNumber int64) (blockHash string, txList []string, txSet map[string]struct{}, err error) {
	hexNum := fmt.Sprintf("0x%x", blockNumber)
	payload := rpcRequest{Jsonrpc: "2.0", Method: "eth_getBlockByNumber", Params: []interface{}{hexNum, false}, ID: 1}
	body, _ := json.Marshal(payload)
	for attempt := 0; attempt < 5; attempt++ {
		resp, e := client.Post(rpcURL, "application/json", bytes.NewReader(body))
		if e != nil {
			if attempt < 4 {
				time.Sleep(time.Duration(1<<attempt) * time.Second)
				continue
			}
			return "", nil, nil, fmt.Errorf("rpc post: %w", e)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var r blockResponse
		if e2 := json.Unmarshal(data, &r); e2 != nil {
			return "", nil, nil, fmt.Errorf("rpc unmarshal: %w", e2)
		}
		if r.Result == nil {
			return "", nil, nil, fmt.Errorf("block %d: null result", blockNumber)
		}
		txSet = make(map[string]struct{}, len(r.Result.Transactions))
		for _, h := range r.Result.Transactions {
			txSet[strings.ToLower(h)] = struct{}{}
		}
		return strings.ToLower(r.Result.Hash), r.Result.Transactions, txSet, nil
	}
	return "", nil, nil, fmt.Errorf("rpc: max retries")
}

// ── Scylla ───────────────────────────────────────────────────────────────────

// storedTxHashes возвращает все (transaction_index, hash) для блока из eth.transactions.
func storedTxHashes(session *gocql.Session, blockNumber int64) (map[string]int, []int, error) {
	chunk := blockToChunk(blockNumber)
	iter := session.Query(
		`SELECT transaction_index, hash FROM eth.transactions WHERE chunk=? AND block_number=?`,
		chunk, blockNumber,
	).Iter()

	hashToIdx := make(map[string]int)
	var indices []int
	var txIdx int
	var hash string
	for iter.Scan(&txIdx, &hash) {
		hashToIdx[strings.ToLower(hash)] = txIdx
		indices = append(indices, txIdx)
	}
	if err := iter.Close(); err != nil {
		return nil, nil, fmt.Errorf("scan block %d: %w", blockNumber, err)
	}
	sort.Ints(indices)
	return hashToIdx, indices, nil
}

// blockCompletionsTxCount возвращает tx_count из eth.block_completions для блока.
func blockCompletionsTxCount(session *gocql.Session, blockNumber int64) (int32, bool, error) {
	chunk := blockToChunk(blockNumber)
	var txCount int32
	err := session.Query(
		`SELECT tx_count FROM eth.block_completions WHERE chunk=? AND block_number=?`,
		chunk, blockNumber,
	).Scan(&txCount)
	if err == gocql.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return txCount, true, nil
}

// ── work item ────────────────────────────────────────────────────────────────

type orphanEntry struct {
	hash string
	idx  int
}

type blockReport struct {
	blockNumber    int64
	canonicalHash  string
	canonicalCount int
	storedCount    int
	bcTxCount      int32
	bcPresent      bool
	orphanOnly     []orphanEntry // stored but not canonical (hash + tx_index)
	lost           []string     // canonical but not stored
	commonCount    int
	err            error
}

func (r *blockReport) status() string {
	if r.err != nil {
		return "ERROR"
	}
	if !r.bcPresent {
		return "NOT_INDEXED"
	}
	if len(r.orphanOnly) == 0 && len(r.lost) == 0 {
		return "OK"
	}
	if len(r.lost) == 0 {
		return "ORPHANED_SUPERSET" // has all canonical + extra orphaned txs
	}
	if len(r.orphanOnly) == 0 {
		return "MISSING_CANONICAL" // missing canonical txs (data integrity issue!)
	}
	return "MIXED" // both orphan_only and lost — fully wrong block
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	blocksStr := flag.String("blocks", "", "comma-separated block numbers to check")
	blocksFile := flag.String("blocks-file", "", "file with one block number per line")
	rpcURL := flag.String("rpc", "http://100.64.0.60:8545", "Ethereum RPC URL")
	host := flag.String("host", "127.0.0.1", "Scylla host")
	port := flag.Int("port", 9042, "Scylla port")
	user := flag.String("user", "cassandra", "Scylla user")
	pass := flag.String("pass", "cassandra", "Scylla password")
	workers := flag.Int("workers", 8, "parallel workers")
	outputFile := flag.String("output", "", "log file path (default: forked_check_TIMESTAMP.log)")
	flag.Parse()

	// collect block numbers
	var blockNums []int64

	if *blocksStr != "" {
		for _, s := range strings.Split(*blocksStr, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				log.Fatalf("invalid block number %q: %v", s, err)
			}
			blockNums = append(blockNums, n)
		}
	}

	if *blocksFile != "" {
		data, err := os.ReadFile(*blocksFile)
		if err != nil {
			log.Fatalf("read blocks-file: %v", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			n, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				log.Printf("skip non-numeric line: %q", line)
				continue
			}
			blockNums = append(blockNums, n)
		}
	}

	if len(blockNums) == 0 {
		fmt.Fprintln(os.Stderr, "no block numbers provided; use --blocks or --blocks-file")
		flag.Usage()
		os.Exit(1)
	}

	// dedup and sort
	seen := make(map[int64]struct{})
	unique := blockNums[:0]
	for _, n := range blockNums {
		if _, ok := seen[n]; !ok {
			seen[n] = struct{}{}
			unique = append(unique, n)
		}
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	blockNums = unique

	// open log file
	logPath := *outputFile
	if logPath == "" {
		logPath = fmt.Sprintf("forked_check_%s.log", time.Now().Format("20060102_150405"))
	}
	logF, err := os.Create(logPath)
	if err != nil {
		log.Fatalf("create log file: %v", err)
	}
	defer logF.Close()

	logf := func(format string, args ...interface{}) {
		line := fmt.Sprintf(format, args...)
		fmt.Print(line)
		fmt.Fprint(logF, line)
	}

	logf("=== forked_block_checker started %s ===\n", time.Now().Format(time.RFC3339))
	logf("blocks: %d  workers: %d  rpc: %s\n\n", len(blockNums), *workers, *rpcURL)

	// scylla
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = "eth"
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 30 * time.Second
	cluster.NumConns = *workers + 2

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("scylla connect: %v", err)
	}
	defer session.Close()

	client := &http.Client{Timeout: 30 * time.Second}

	// fan-out
	type work struct{ blockNumber int64 }
	workCh := make(chan work, len(blockNums))
	for _, n := range blockNums {
		workCh <- work{n}
	}
	close(workCh)

	results := make([]blockReport, len(blockNums))
	idxMap := make(map[int64]int, len(blockNums))
	for i, n := range blockNums {
		idxMap[n] = i
	}

	var done atomic.Int64
	var mu sync.Mutex

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workCh {
				bn := item.blockNumber
				rep := blockReport{blockNumber: bn}

				// canonical
				canonHash, _, canonSet, e := getCanonicalBlock(client, *rpcURL, bn)
				if e != nil {
					rep.err = fmt.Errorf("rpc: %w", e)
					mu.Lock()
					results[idxMap[bn]] = rep
					mu.Unlock()
					done.Add(1)
					continue
				}
				rep.canonicalHash = canonHash
				rep.canonicalCount = len(canonSet)

				// scylla block_completions
				bcTxCount, bcPresent, e := blockCompletionsTxCount(session, bn)
				if e != nil {
					rep.err = fmt.Errorf("bc: %w", e)
					mu.Lock()
					results[idxMap[bn]] = rep
					mu.Unlock()
					done.Add(1)
					continue
				}
				rep.bcPresent = bcPresent
				rep.bcTxCount = bcTxCount

				if !bcPresent {
					mu.Lock()
					results[idxMap[bn]] = rep
					mu.Unlock()
					done.Add(1)
					continue
				}

				// scylla transactions
				hashToIdx, _, e := storedTxHashes(session, bn)
				if e != nil {
					rep.err = fmt.Errorf("txhashes: %w", e)
					mu.Lock()
					results[idxMap[bn]] = rep
					mu.Unlock()
					done.Add(1)
					continue
				}
				rep.storedCount = len(hashToIdx)

				// diff
				for h, idx := range hashToIdx {
					if _, ok := canonSet[h]; !ok {
						rep.orphanOnly = append(rep.orphanOnly, orphanEntry{hash: h, idx: idx})
					} else {
						rep.commonCount++
					}
				}
				for h := range canonSet {
					if _, ok := hashToIdx[h]; !ok {
						rep.lost = append(rep.lost, h)
					}
				}
				sort.Slice(rep.orphanOnly, func(i, j int) bool { return rep.orphanOnly[i].idx < rep.orphanOnly[j].idx })
				sort.Strings(rep.lost)

				mu.Lock()
				results[idxMap[bn]] = rep
				mu.Unlock()

				cnt := done.Add(1)
				if cnt%10 == 0 || cnt == int64(len(blockNums)) {
					fmt.Printf("\rprogress: %d/%d", cnt, len(blockNums))
				}
			}
		}()
	}
	wg.Wait()
	fmt.Println()

	// write detailed report
	var (
		cntOK              int
		cntNotIndexed      int
		cntOrphanedSuperset int
		cntMissingCanon    int
		cntMixed           int
		cntError           int
	)

	logf("\n=== DETAILED RESULTS ===\n\n")
	for _, rep := range results {
		st := rep.status()
		switch st {
		case "OK":
			cntOK++
			logf("BLOCK %d  status=OK  canonical=%d  stored=%d  bc_tx_count=%d  canonical_hash=%s\n",
				rep.blockNumber, rep.canonicalCount, rep.storedCount, rep.bcTxCount, rep.canonicalHash)
		case "NOT_INDEXED":
			cntNotIndexed++
			logf("BLOCK %d  status=NOT_INDEXED  canonical=%d  canonical_hash=%s\n",
				rep.blockNumber, rep.canonicalCount, rep.canonicalHash)
		case "ORPHANED_SUPERSET":
			cntOrphanedSuperset++
			logf("BLOCK %d  status=ORPHANED_SUPERSET  canonical=%d  stored=%d  bc_tx_count=%d  orphan_only=%d  lost=0\n",
				rep.blockNumber, rep.canonicalCount, rep.storedCount, rep.bcTxCount, len(rep.orphanOnly))
			logf("  canonical_hash=%s\n", rep.canonicalHash)
			logf("  ORPHAN_ONLY_HASHES (%d):\n", len(rep.orphanOnly))
			for _, e := range rep.orphanOnly {
				logf("    tx_idx=%-4d  %s\n", e.idx, e.hash)
			}
		case "MISSING_CANONICAL":
			cntMissingCanon++
			logf("BLOCK %d  status=MISSING_CANONICAL  canonical=%d  stored=%d  bc_tx_count=%d  orphan_only=0  lost=%d  *** DATA INTEGRITY ISSUE ***\n",
				rep.blockNumber, rep.canonicalCount, rep.storedCount, rep.bcTxCount, len(rep.lost))
			logf("  canonical_hash=%s\n", rep.canonicalHash)
			logf("  LOST_HASHES (%d):\n", len(rep.lost))
			for _, h := range rep.lost {
				logf("    %s\n", h)
			}
		case "MIXED":
			cntMixed++
			logf("BLOCK %d  status=MIXED  canonical=%d  stored=%d  bc_tx_count=%d  orphan_only=%d  lost=%d  *** DATA INTEGRITY ISSUE ***\n",
				rep.blockNumber, rep.canonicalCount, rep.storedCount, rep.bcTxCount, len(rep.orphanOnly), len(rep.lost))
			logf("  canonical_hash=%s\n", rep.canonicalHash)
			logf("  ORPHAN_ONLY_HASHES (%d):\n", len(rep.orphanOnly))
			for _, e := range rep.orphanOnly {
				logf("    tx_idx=%-4d  %s\n", e.idx, e.hash)
			}
			logf("  LOST_HASHES (%d):\n", len(rep.lost))
			for _, h := range rep.lost {
				logf("    %s\n", h)
			}
		case "ERROR":
			cntError++
			logf("BLOCK %d  status=ERROR  err=%v\n", rep.blockNumber, rep.err)
		}
	}

	logf("\n=== SUMMARY ===\n")
	logf("total blocks checked:    %d\n", len(blockNums))
	logf("OK (clean):              %d\n", cntOK)
	logf("NOT_INDEXED (no bc):     %d\n", cntNotIndexed)
	logf("ORPHANED_SUPERSET:       %d  (has all canonical + orphaned extras — cosmetic issue, no data loss)\n", cntOrphanedSuperset)
	logf("MISSING_CANONICAL:       %d  *** DATA INTEGRITY ISSUE — missing canonical txs ***\n", cntMissingCanon)
	logf("MIXED:                   %d  *** DATA INTEGRITY ISSUE — wrong block indexed ***\n", cntMixed)
	logf("ERROR:                   %d\n", cntError)
	logf("\nlog written to: %s\n", logPath)
	logf("finished: %s\n", time.Now().Format(time.RFC3339))
}
