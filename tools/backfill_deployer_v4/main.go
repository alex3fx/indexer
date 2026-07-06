// backfill_deployer_v4: correct-semantics deployer backfill for null rows
// not covered by the snap table (re-deployed addresses, factory creates).
//
// Canonical semantics (from historical.ts):
//   deployer        = tx.from_address  (EOA, the outer tx sender)
//   contract_factory = trace.action.from ONLY when != EOA; null for direct deploys
//
// Strategy:
//   Phase 1 (~40 min): scan contracts_by_address_v2 for null deployer rows,
//     collecting (address, block_number, tx_hash).
//   Phase 2a: for each null row with tx_hash, compute chunk and query
//     eth.transactions WHERE chunk=? AND block_number=? to get from_address (EOA).
//     chunk formula: (block % 24) + 24 * (block / 12000)
//   Phase 2b: call trace_filter for 100-block windows containing null rows
//     to get trace.action.from per (address, block) — used for contract_factory.
//   Phase 3: write deployer=EOA, tx_hash, contract_factory to contracts_by_address_v2.
//
// Safe to run after backfill_restore_from_snap — only touches rows with null deployer.
//
// Usage: backfill_deployer_v4 --host 127.0.0.1 --pass cassandra [flags]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

// chunk formula for eth.transactions: LANES=24, ERA=12000
func blockToChunk(block int64) int {
	return int(block%24) + 24*int(block/12000)
}

type nullRow struct {
	address string
	block   int64
	txHash  string
}

type traceKey struct {
	address string
	block   int64
}

type updateWork struct {
	address  string
	block    int64
	deployer string
	txHash   string
	factory  string // empty = null
}

type traceAction struct {
	From string `json:"from"`
}
type traceResult struct {
	Address string `json:"address"`
}
type traceEntry struct {
	Type    string      `json:"type"`
	Action  traceAction `json:"action"`
	Result  traceResult `json:"result"`
	TxHash  string      `json:"transactionHash"`
	BlockNo int64       `json:"blockNumber"`
	Error   string      `json:"error"`
}
type rpcResp struct {
	Result []traceEntry `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

var (
	rpcURL     string
	httpClient *http.Client
)

func main() {
	host       := flag.String("host", "127.0.0.1", "Scylla host")
	port       := flag.Int("port", 9042, "Scylla port")
	user       := flag.String("user", "cassandra", "Scylla user")
	pass       := flag.String("pass", "", "Scylla password")
	rpc        := flag.String("rpc", "http://100.64.0.60:8545", "ETH RPC URL")
	rpcWorkers := flag.Int("rpc-workers", 30, "concurrent trace_filter workers")
	txWorkers  := flag.Int("tx-workers", 64, "concurrent eth.transactions lookup workers")
	dbWorkers  := flag.Int("db-workers", 64, "concurrent UPDATE workers")
	pageSize   := flag.Int("page-size", 5000, "scan page size")
	dryRun     := flag.Bool("dry-run", false, "scan only, don't write to DB")
	flag.Parse()

	rpcURL = *rpc
	httpClient = &http.Client{Timeout: 120 * time.Second}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalQuorum
	cluster.NumConns = 16
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// ── Phase 1: collect null rows ────────────────────────────────────────────
	log.Printf("=== Phase 1: scan contracts_by_address_v2 for null deployer ===")
	p1Start := time.Now()

	var nullRows []nullRow
	windowSet := make(map[int64]bool)
	var p1Scanned, p1Nulls atomic.Int64

	stopP1 := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopP1:
				return
			case <-time.After(30 * time.Second):
				log.Printf("[phase1] scanned=%d nulls=%d", p1Scanned.Load(), p1Nulls.Load())
			}
		}
	}()

	const selV2 = `SELECT address, block_number, deployer, tx_hash FROM eth.contracts_by_address_v2`
	iter := session.Query(selV2).PageSize(*pageSize).Iter()
	var addr, dep, txh string
	var blk int64
	for iter.Scan(&addr, &blk, &dep, &txh) {
		p1Scanned.Add(1)
		if dep == "" {
			p1Nulls.Add(1)
			nullRows = append(nullRows, nullRow{strings.ToLower(addr), blk, strings.ToLower(txh)})
			windowSet[(blk/100)*100] = true
		}
		dep = ""
		txh = ""
	}
	if err2 := iter.Close(); err2 != nil {
		log.Printf("[phase1] iter err: %v", err2)
	}
	close(stopP1)

	p1Elapsed := time.Since(p1Start).Round(time.Second)
	log.Printf("=== Phase 1 done: scanned=%d nulls=%d windows=%d elapsed=%s ===",
		p1Scanned.Load(), p1Nulls.Load(), len(windowSet), p1Elapsed)

	if len(nullRows) == 0 {
		fmt.Println("=== DONE === 0 nulls — nothing to do")
		return
	}

	// ── Phase 2a: resolve EOA from eth.transactions ───────────────────────────
	log.Printf("=== Phase 2a: resolve EOA from eth.transactions (tx-workers=%d) ===", *txWorkers)
	p2aStart := time.Now()

	// Group by (chunk, block_number) for efficient partition reads
	type blockKey struct{ chunk, block int }
	blockToRows := make(map[blockKey][]*nullRow, len(nullRows))
	for i := range nullRows {
		if nullRows[i].txHash == "" {
			continue
		}
		bk := blockKey{blockToChunk(nullRows[i].block), int(nullRows[i].block)}
		blockToRows[bk] = append(blockToRows[bk], &nullRows[i])
	}

	// EOA lookup result: txHash → from_address
	eoaMu := sync.Mutex{}
	eoaMap := make(map[string]string, len(nullRows))
	var eoaResolved, eoaMissing atomic.Int64

	blockKeyCh := make(chan blockKey, len(blockToRows))
	for bk := range blockToRows {
		blockKeyCh <- bk
	}
	close(blockKeyCh)

	var wgTx sync.WaitGroup
	const selTx = `SELECT hash, from_address FROM eth.transactions WHERE chunk = ? AND block_number = ?`
	for i := 0; i < *txWorkers; i++ {
		wgTx.Add(1)
		go func() {
			defer wgTx.Done()
			for bk := range blockKeyCh {
				needed := make(map[string]bool)
				for _, r := range blockToRows[bk] {
					if r.txHash != "" {
						needed[r.txHash] = true
					}
				}
				txIter := session.Query(selTx, bk.chunk, bk.block).Iter()
				var h, from string
				for txIter.Scan(&h, &from) {
					hl := strings.ToLower(h)
					if needed[hl] {
						eoaMu.Lock()
						eoaMap[hl] = strings.ToLower(from)
						eoaMu.Unlock()
						eoaResolved.Add(1)
					}
				}
				if err2 := txIter.Close(); err2 != nil {
					log.Printf("[2a] scan chunk=%d block=%d: %v", bk.chunk, bk.block, err2)
				}
			}
		}()
	}
	wgTx.Wait()

	for i := range nullRows {
		if nullRows[i].txHash == "" {
			eoaMissing.Add(1)
		} else if eoaMap[nullRows[i].txHash] == "" {
			eoaMissing.Add(1)
		}
	}
	log.Printf("=== Phase 2a done: resolved=%d missing=%d elapsed=%s ===",
		eoaResolved.Load(), eoaMissing.Load(), time.Since(p2aStart).Round(time.Second))

	// ── Phase 2b: trace_filter for windows containing null rows ───────────────
	log.Printf("=== Phase 2b: trace_filter for %d windows (rpc-workers=%d) ===",
		len(windowSet), *rpcWorkers)
	p2bStart := time.Now()

	windows := make([]int64, 0, len(windowSet))
	for w := range windowSet {
		windows = append(windows, w)
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })

	// Store action.from and tx_hash per create trace: (addr, block) → (actionFrom, txHash)
	type traceVal struct {
		actionFrom string
		txHash     string
	}
	traceMu := sync.Mutex{}
	traceMap := make(map[traceKey]traceVal, len(nullRows))
	var p2bDone, p2bCreates atomic.Int64

	winCh := make(chan int64, len(windows))
	for _, w := range windows {
		winCh <- w
	}
	close(winCh)

	go func() {
		total := int64(len(windows))
		for {
			time.Sleep(30 * time.Second)
			d := p2bDone.Load()
			log.Printf("[phase2b] windows=%d/%d creates=%d", d, total, p2bCreates.Load())
			if d >= total {
				return
			}
		}
	}()

	var wgRPC sync.WaitGroup
	for i := 0; i < *rpcWorkers; i++ {
		wgRPC.Add(1)
		go func() {
			defer wgRPC.Done()
			for win := range winCh {
				entries := traceFilterRange(win, win+99)
				for _, t := range entries {
					if t.Type != "create" || t.Error != "" || t.Result.Address == "" {
						continue
					}
					p2bCreates.Add(1)
					k := traceKey{strings.ToLower(t.Result.Address), t.BlockNo}
					v := traceVal{strings.ToLower(t.Action.From), strings.ToLower(t.TxHash)}
					traceMu.Lock()
					traceMap[k] = v
					traceMu.Unlock()
				}
				p2bDone.Add(1)
			}
		}()
	}
	wgRPC.Wait()
	log.Printf("=== Phase 2b done: windows=%d creates=%d elapsed=%s ===",
		p2bDone.Load(), p2bCreates.Load(), time.Since(p2bStart).Round(time.Second))

	// ── Phase 3: build and write updates ─────────────────────────────────────
	log.Printf("=== Phase 3: writing (db-workers=%d dry-run=%v) ===", *dbWorkers, *dryRun)
	p3Start := time.Now()

	updateCh := make(chan updateWork, *dbWorkers*4)
	var wgDB sync.WaitGroup
	var p3Updated, p3DbErr, p3Skipped atomic.Int64

	const updWith    = `UPDATE eth.contracts_by_address_v2 SET deployer = ?, tx_hash = ?, contract_factory = ? WHERE address = ? AND block_number = ?`
	const updWithout = `UPDATE eth.contracts_by_address_v2 SET deployer = ?, tx_hash = ? WHERE address = ? AND block_number = ?`

	for i := 0; i < *dbWorkers; i++ {
		wgDB.Add(1)
		go func() {
			defer wgDB.Done()
			for uw := range updateCh {
				if *dryRun {
					p3Updated.Add(1)
					continue
				}
				var execErr error
				if uw.factory != "" {
					execErr = retryDB(func() error {
						return session.Query(updWith, uw.deployer, uw.txHash, uw.factory,
							uw.address, uw.block).Exec()
					})
				} else {
					execErr = retryDB(func() error {
						return session.Query(updWithout, uw.deployer, uw.txHash,
							uw.address, uw.block).Exec()
					})
				}
				if execErr != nil {
					p3DbErr.Add(1)
					log.Printf("[err] update addr=%s blk=%d: %v", uw.address, uw.block, execErr)
				} else {
					p3Updated.Add(1)
				}
			}
		}()
	}

	for i := range nullRows {
		row := &nullRows[i]
		eoa := eoaMap[row.txHash]
		tv := traceMap[traceKey{row.address, row.block}]

		deployer := eoa
		if deployer == "" {
			// No tx lookup hit — for direct deploys action.from == EOA, use it
			deployer = tv.actionFrom
		}
		if deployer == "" {
			p3Skipped.Add(1)
			log.Printf("[skip] no deployer data addr=%s blk=%d", row.address, row.block)
			continue
		}

		txH := row.txHash
		if txH == "" {
			txH = tv.txHash
		}

		factory := ""
		if tv.actionFrom != "" && tv.actionFrom != deployer {
			factory = tv.actionFrom
		}

		updateCh <- updateWork{
			address:  row.address,
			block:    row.block,
			deployer: deployer,
			txHash:   txH,
			factory:  factory,
		}
	}

	close(updateCh)
	wgDB.Wait()

	fmt.Println("=== DONE ===")
	fmt.Printf("phase1:  scanned=%d  nulls=%d  windows=%d  elapsed=%s\n",
		p1Scanned.Load(), p1Nulls.Load(), len(windowSet), p1Elapsed)
	fmt.Printf("phase2a: eoaResolved=%d  eoaMissing=%d  elapsed=%s\n",
		eoaResolved.Load(), eoaMissing.Load(), time.Since(p2aStart).Round(time.Second))
	fmt.Printf("phase2b: windows=%d  creates=%d  elapsed=%s\n",
		p2bDone.Load(), p2bCreates.Load(), time.Since(p2bStart).Round(time.Second))
	fmt.Printf("phase3:  updated=%d  skipped=%d  dbErr=%d  elapsed=%s\n",
		p3Updated.Load(), p3Skipped.Load(), p3DbErr.Load(), time.Since(p3Start).Round(time.Second))
	if p3DbErr.Load() > 0 {
		fmt.Printf("WARNING: %d DB errors — re-run to retry\n", p3DbErr.Load())
	} else if p3Skipped.Load() > 0 {
		fmt.Printf("WARNING: %d rows skipped (no source data)\n", p3Skipped.Load())
	} else {
		fmt.Println("OK — no errors")
	}
}

func traceFilterRange(from, to int64) []traceEntry {
	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"trace_filter","params":[{"fromBlock":"0x%x","toBlock":"0x%x"}],"id":1}`,
		from, to,
	)
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := callRPC(body)
		if err != nil {
			if attempt == 3 {
				log.Printf("[rpc-err] [%d,%d]: %v", from, to, err)
				return nil
			}
			time.Sleep(delay)
			delay = minDur(delay*2, 30*time.Second)
			continue
		}
		if resp.Error != nil {
			msg := resp.Error.Message
			if strings.Contains(msg, "too big") || strings.Contains(msg, "Response is too big") ||
				strings.Contains(msg, "too large") {
				if to-from <= 1 {
					log.Printf("[rpc-err] [%d,%d] too big, can't split", from, to)
					return nil
				}
				mid := (from + to) / 2
				return append(traceFilterRange(from, mid), traceFilterRange(mid+1, to)...)
			}
			if attempt == 3 {
				log.Printf("[rpc-err] [%d,%d]: %s", from, to, msg)
				return nil
			}
			time.Sleep(delay)
			delay = minDur(delay*2, 30*time.Second)
			continue
		}
		return resp.Result
	}
	return nil
}

func callRPC(body string) (*rpcResp, error) {
	req, _ := http.NewRequest("POST", rpcURL, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	httpResp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	var r rpcResp
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("unmarshal: %w (body: %.200s)", err, data)
	}
	return &r, nil
}

func retryDB(fn func() error) error {
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else if attempt == 7 {
			return fmt.Errorf("after 8 retries: %w", err)
		}
		jitter := time.Duration(float64(delay) * (0.8 + 0.4*math.Abs(math.Sin(float64(attempt)))))
		if jitter < 100*time.Millisecond {
			jitter = 100 * time.Millisecond
		}
		time.Sleep(jitter)
		delay = minDur(delay*2, 30*time.Second)
	}
	return nil
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
