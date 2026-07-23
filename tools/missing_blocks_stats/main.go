// missing_blocks_stats:
//   Phase 1: scans block_completions to find missing blocks and collect BC sums
//   Phase 2: for each missing block, queries ETH node (reth) for true tx/log/itx counts
//   Final:   BC_sum + missing_from_node == ? → compare vs actual count_rows result
//
// RPC calls per missing block:
//   eth_getBlockByNumber   → tx count
//   eth_getBlockReceipts   → log count (sum of receipt.logs lengths)
//   trace_block            → itx count (same filter as transformer: from!="", value!=null, has tx)
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
	defaultLanes   = 24
	defaultEraSize = 12000
)

// ── JSON-RPC helpers ──────────────────────────────────────────────────────────

func rpcCall(client *http.Client, rpcURL, method string, params interface{}) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": method, "params": params,
	})
	resp, err := client.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var result struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("json unmarshal: %v", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", result.Error.Message)
	}
	return result.Result, nil
}

func blockHex(n int64) string { return fmt.Sprintf("0x%x", n) }

type blockCounts struct {
	block  int64
	tx     int64
	log    int64
	itx    int64
	rpcErr bool
}

func getBlockCounts(client *http.Client, rpcURL string, blockNum int64, retries int) blockCounts {
	var delay = 500 * time.Millisecond
	bh := blockHex(blockNum)

	for attempt := 0; attempt < retries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 16*time.Second { delay *= 2 }
		}

		// TX count via eth_getBlockByNumber (tx hashes only)
		raw, err := rpcCall(client, rpcURL, "eth_getBlockByNumber", []interface{}{bh, false})
		if err != nil { log.Printf("[block %d tx] attempt %d: %v", blockNum, attempt+1, err); continue }
		var blk struct {
			Transactions []string `json:"transactions"`
		}
		if err := json.Unmarshal(raw, &blk); err != nil { continue }
		txCount := int64(len(blk.Transactions))

		// Log count via eth_getBlockReceipts
		raw, err = rpcCall(client, rpcURL, "eth_getBlockReceipts", []interface{}{bh})
		if err != nil { log.Printf("[block %d logs] attempt %d: %v", blockNum, attempt+1, err); continue }
		var receipts []struct {
			Logs []json.RawMessage `json:"logs"`
		}
		if err := json.Unmarshal(raw, &receipts); err != nil { continue }
		var logCount int64
		for _, r := range receipts { logCount += int64(len(r.Logs)) }

		// ITX count via trace_block (same filter as transformer)
		raw, err = rpcCall(client, rpcURL, "trace_block", []interface{}{bh})
		if err != nil { log.Printf("[block %d itx] attempt %d: %v", blockNum, attempt+1, err); continue }
		var traces []struct {
			Action struct {
				From  string  `json:"from"`
				Value *string `json:"value"`
			} `json:"action"`
			TransactionPosition *int    `json:"transactionPosition"`
			TransactionHash     *string `json:"transactionHash"`
		}
		if err := json.Unmarshal(raw, &traces); err != nil { continue }
		var itxCount int64
		for _, t := range traces {
			// Skip traces with no transaction (miner reward etc)
			if t.TransactionPosition == nil && (t.TransactionHash == nil || *t.TransactionHash == "") {
				continue
			}
			// Transformer filter: from != "" AND value != null
			if t.Action.From == "" { continue }
			if t.Action.Value == nil { continue }
			itxCount++
		}

		return blockCounts{blockNum, txCount, logCount, itxCount, false}
	}
	log.Printf("[ERROR] block %d: all %d retries failed", blockNum, retries)
	return blockCounts{blockNum, 0, 0, 0, true}
}

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "port")
	user     := flag.String("user", "cassandra", "user")
	pass     := flag.String("pass", "", "password")
	ks       := flag.String("keyspace", "eth", "keyspace")
	from     := flag.Int64("from", 0, "start block (inclusive)")
	to       := flag.Int64("to", 0, "end block (inclusive)")
	workers  := flag.Int("workers", 12, "parallel chunk workers (Phase 1)")
	rpcWorkers := flag.Int("rpc-workers", 16, "parallel RPC workers (Phase 2)")
	timeout  := flag.Duration("timeout", 120*time.Second, "Scylla query timeout")
	retries  := flag.Int("retries", 5, "retries per query")
	lanes    := flag.Int64("lanes", defaultLanes, "chunk lanes")
	eraSize  := flag.Int64("era-size", defaultEraSize, "blocks per era")
	rpcURL   := flag.String("rpc", "http://100.64.0.60:8545", "ETH node RPC URL")
	flag.Parse()

	if *pass == "" { log.Fatal("--pass required") }
	if *to == 0   { log.Fatal("--to required") }

	// Scylla session
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *ks
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = *timeout
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 8
	session, err := cluster.CreateSession()
	if err != nil { log.Fatalf("connect: %v", err) }
	defer session.Close()

	// HTTP client for RPC
	httpClient := &http.Client{Timeout: 30 * time.Second}

	minEra := *from / *eraSize
	maxEra := *to / *eraSize
	var chunks []int
	for era := minEra; era <= maxEra; era++ {
		for lane := int64(0); lane < *lanes; lane++ {
			chunks = append(chunks, int(lane+*lanes*era))
		}
	}
	log.Printf("from=%d to=%d eras=%d-%d chunks=%d rpc=%s", *from, *to, minEra, maxEra, len(chunks), *rpcURL)

	// ── Phase 1: scan block_completions ──────────────────────────────────────
	log.Println("Phase 1: scanning block_completions...")
	p1Start := time.Now()

	type bcEntry struct{ tx, log, itx int64 }
	allBC := make(map[int64]bcEntry, 1100000)
	var mu sync.Mutex
	var p1Errs atomic.Int64
	var p1Done atomic.Int64

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks { chunkCh <- c }
	close(chunkCh)

	bcQ := `SELECT block_number, tx_count, log_count, itx_count FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?`

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				local := make(map[int64]bcEntry)
				var delay = 500 * time.Millisecond
				ok := false
				for attempt := 0; attempt < *retries; attempt++ {
					if attempt > 0 {
						time.Sleep(delay)
						if delay < 32*time.Second { delay *= 2 }
					}
					iter := session.Query(bcQ, chunk, *from, *to).Iter()
					local = make(map[int64]bcEntry)
					var bn, tx, lg, itx int64
					for iter.Scan(&bn, &tx, &lg, &itx) {
						local[bn] = bcEntry{tx, lg, itx}
					}
					if err := iter.Close(); err != nil {
						log.Printf("[retry %d/%d] BC chunk=%d: %v", attempt+1, *retries, chunk, err)
						continue
					}
					ok = true
					break
				}
				if !ok { p1Errs.Add(1) }
				mu.Lock()
				for bn, e := range local { allBC[bn] = e }
				mu.Unlock()
				p1Done.Add(1)
			}
		}()
	}

	tick := time.NewTicker(10 * time.Second)
	go func() {
		for range tick.C {
			log.Printf("Phase 1: %d/%d chunks (%.0fs)", p1Done.Load(), len(chunks), time.Since(p1Start).Seconds())
		}
	}()
	wg.Wait()
	tick.Stop()

	var bcTx, bcLog, bcItx int64
	for _, e := range allBC {
		bcTx += e.tx; bcLog += e.log; bcItx += e.itx
	}

	var missing []int64
	for b := *from; b <= *to; b++ {
		if _, ok := allBC[b]; !ok { missing = append(missing, b) }
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })

	log.Printf("Phase 1 done: BC entries=%d missing=%d errors=%d (%.1fs)",
		len(allBC), len(missing), p1Errs.Load(), time.Since(p1Start).Seconds())
	log.Printf("BC sums: tx=%d log=%d itx=%d", bcTx, bcLog, bcItx)

	if len(missing) == 0 {
		fmt.Printf("\nNo missing blocks in [%d, %d] — BC complete.\n", *from, *to)
		fmt.Printf("BC tx=%d  log=%d  itx=%d\n", bcTx, bcLog, bcItx)
		return
	}

	// Print missing block list
	fmt.Printf("\nMissing blocks (%d total):\n", len(missing))
	for i, b := range missing {
		if i < 30 || i >= len(missing)-5 {
			fmt.Printf("  %d\n", b)
		} else if i == 30 {
			fmt.Printf("  ... (%d more) ...\n", len(missing)-35)
		}
	}

	// ── Phase 2: query RPC for each missing block ─────────────────────────────
	log.Printf("Phase 2: querying reth for %d missing blocks (workers=%d)...", len(missing), *rpcWorkers)
	p2Start := time.Now()

	missingCh := make(chan int64, len(missing))
	for _, b := range missing { missingCh <- b }
	close(missingCh)

	countsCh := make(chan blockCounts, len(missing))
	var wg2 sync.WaitGroup
	var p2Done atomic.Int64
	for i := 0; i < *rpcWorkers; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for b := range missingCh {
				countsCh <- getBlockCounts(httpClient, *rpcURL, b, *retries)
				p2Done.Add(1)
				if p2Done.Load()%50 == 0 {
					log.Printf("Phase 2: %d/%d blocks (%.0fs)", p2Done.Load(), len(missing), time.Since(p2Start).Seconds())
				}
			}
		}()
	}
	go func() { wg2.Wait(); close(countsCh) }()

	var missingTx, missingLog, missingItx int64
	var rpcErrCount int
	var detailLines []string
	for bc := range countsCh {
		if bc.rpcErr {
			rpcErrCount++
			detailLines = append(detailLines, fmt.Sprintf("  block %-10d  ERROR", bc.block))
		} else {
			missingTx += bc.tx; missingLog += bc.log; missingItx += bc.itx
			detailLines = append(detailLines, fmt.Sprintf("  block %-10d  tx=%-6d log=%-6d itx=%-6d", bc.block, bc.tx, bc.log, bc.itx))
		}
	}
	sort.Strings(detailLines)

	log.Printf("Phase 2 done: tx=%d log=%d itx=%d rpc_errors=%d (%.1fs)",
		missingTx, missingLog, missingItx, rpcErrCount, time.Since(p2Start).Seconds())

	// ── Final report ──────────────────────────────────────────────────────────
	fmt.Printf("\nPer-block counts from node:\n")
	for _, l := range detailLines { fmt.Println(l) }

	adjTx  := bcTx  + missingTx
	adjLog := bcLog + missingLog
	adjItx := bcItx + missingItx

	fmt.Printf("\n=== FINAL COMPARISON from=%d to=%d ===\n", *from, *to)
	fmt.Printf("%-35s  %15s  %15s  %15s\n", "", "transactions", "logs", "internal_txs")
	fmt.Printf("%-35s  %15d  %15d  %15d\n", "BC sum (present blocks)", bcTx, bcLog, bcItx)
	fmt.Printf("%-35s  %15d  %15d  %15d\n", "Missing blocks (from node)", missingTx, missingLog, missingItx)
	fmt.Printf("%-35s  %15d  %15d  %15d\n", "ADJUSTED TOTAL (BC + missing)", adjTx, adjLog, adjItx)
	fmt.Printf("\n")
	fmt.Printf("Compare ADJUSTED TOTAL with actual count_rows for [%d, %d]:\n", *from, *to)
	fmt.Printf("  count_rows TX  = 192740864   (era 13M, known)\n")
	fmt.Printf("  count_rows LOG = (run separately)\n")
	fmt.Printf("  count_rows ITX = (run separately)\n")
	fmt.Printf("\nPhase 1 errors: %d  RPC errors: %d  Total elapsed: %.1fs\n",
		int(p1Errs.Load()), rpcErrCount, time.Since(p1Start).Seconds())
}
