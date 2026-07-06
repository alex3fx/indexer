// backfill_deployer_v2: fills deployer and tx_hash in contracts_by_address_v2 for historical
// rows where these fields are NULL (created by merge_old_to_v2 UPDATE-only backfill).
//
// Strategy: iterate blocks [from, to] in sequential chunks; for each chunk call
// trace_filter(fromBlock, toBlock) which returns ALL trace types. Filter for type="create"
// to find contract deployments. UPDATE contracts_by_address_v2 with deployer/tx_hash.
//
// This is 100-200x faster than individual trace_block calls (50K vs 11M round trips).
// trace_filter with fromBlock/toBlock is supported by this Erigon node (confirmed).
// The action filter ({"action":["create"]}) is NOT supported — we filter client-side.
//
// Safe to re-run: UPDATE is idempotent. Already-populated rows get the same value written.
//
// Usage:
//
//	./backfill_deployer_v2 --rpc=http://100.64.0.60:8545 --host=127.0.0.1 --pass=cassandra \
//	    [--from=0] [--to=25421495] [--range=100] [--workers=200] [--db-workers=256] \
//	    [--log-every=10000]
package main

import (
	"bytes"
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

// ── RPC types ─────────────────────────────────────────────────────────────────

type traceAction struct {
	From string `json:"from"` // deployer address
	Init string `json:"init"` // creation bytecode (ignored here)
}

type traceResult struct {
	Address string `json:"address"` // deployed contract address
	GasUsed string `json:"gasUsed"`
}

type traceEntry struct {
	Type            string      `json:"type"`   // "create", "call", "suicide", "reward"
	Action          traceAction `json:"action"`
	Result          traceResult `json:"result"`
	TransactionHash string      `json:"transactionHash"`
	BlockNumber     int64       `json:"blockNumber"`
	Error           string      `json:"error"` // non-empty on failed traces
}

// ── globals ───────────────────────────────────────────────────────────────────

var (
	rpcURL  string
	session *gocql.Session

	rangesTotal  int64
	rangesDone   atomic.Int64
	tracesTotal  atomic.Int64 // all trace entries received
	creates      atomic.Int64 // type="create" entries
	updated      atomic.Int64 // successful DB UPDATEs
	rpcErrors    atomic.Int64
	dbErrors     atomic.Int64
)

const updDeployerQ = `UPDATE contracts_by_address_v2 SET deployer = ?, tx_hash = ? WHERE address = ? AND block_number = ?`

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	host      := flag.String("host", "127.0.0.1", "Scylla host")
	port      := flag.Int("port", 9042, "Scylla port")
	user      := flag.String("user", "cassandra", "Scylla user")
	pass      := flag.String("pass", "", "Scylla password (required)")
	keyspace  := flag.String("keyspace", "eth", "Scylla keyspace")
	rpc       := flag.String("rpc", "", "ETH JSON-RPC URL (required)")
	fromBlock := flag.Int64("from", 0, "start block (inclusive)")
	toBlock   := flag.Int64("to", 25421495, "end block (inclusive)")
	rangeSize := flag.Int64("range", 100, "blocks per trace_filter call")
	workers   := flag.Int("workers", 200, "concurrent trace_filter goroutines")
	dbWkrs    := flag.Int("db-workers", 512, "concurrent Scylla UPDATE goroutines")
	logEvery  := flag.Int64("log-every", 5000, "log progress every N ranges")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass is required")
	}
	if *rpc == "" {
		log.Fatal("--rpc is required")
	}
	rpcURL = *rpc

	// ── Scylla session ────────────────────────────────────────────────────
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 24
	cluster.Timeout = 120 * time.Second

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect scylla: %v", err)
	}
	defer session.Close()

	// Build range list: [fromBlock, fromBlock+rangeSize-1], [fromBlock+rangeSize, ...]
	type blockRange struct{ from, to int64 }
	var ranges []blockRange
	for b := *fromBlock; b <= *toBlock; b += *rangeSize {
		end := b + *rangeSize - 1
		if end > *toBlock {
			end = *toBlock
		}
		ranges = append(ranges, blockRange{b, end})
	}
	rangesTotal = int64(len(ranges))

	log.Printf("=== DEPLOYER BACKFILL v2 ===")
	log.Printf("blocks=%d→%d range_size=%d total_ranges=%d workers=%d db_workers=%d",
		*fromBlock, *toBlock, *rangeSize, rangesTotal, *workers, *dbWkrs)

	start := time.Now()

	// ── channels ──────────────────────────────────────────────────────────
	rangeCh  := make(chan blockRange, *workers*4)
	updateCh := make(chan updateWork, *dbWkrs*4)

	// DB writer goroutines.
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

	// trace_filter worker goroutines.
	var traceWg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		traceWg.Add(1)
		go func() {
			defer traceWg.Done()
			for r := range rangeCh {
				processRange(r.from, r.to, updateCh)
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
				done := rangesDone.Load()
				total := rangesTotal
				pct := float64(done) / float64(total) * 100
				elapsed := time.Since(start)
				var eta string
				if done > 0 {
					remaining := time.Duration(float64(elapsed) / float64(done) * float64(total-done))
					eta = remaining.Round(time.Second).String()
				} else {
					eta = "?"
				}
				log.Printf("[progress] ranges=%d/%d (%.1f%%) traces=%d creates=%d updated=%d rpcErr=%d dbErr=%d eta=%s",
					done, total, pct,
					tracesTotal.Load(), creates.Load(), updated.Load(),
					rpcErrors.Load(), dbErrors.Load(), eta)
			}
		}
	}()

	// Log a milestone every logEvery ranges.
	var logMu sync.Mutex
	logCounter := int64(0)
	_ = logEvery
	_ = logMu
	_ = logCounter

	// Feed ranges to workers.
	for _, r := range ranges {
		rangeCh <- r
	}
	close(rangeCh)

	traceWg.Wait()
	close(updateCh)
	writerWg.Wait()
	close(stopProgress)

	elapsed := time.Since(start).Round(time.Second)
	log.Printf("=== DONE === ranges=%d traces=%d creates=%d updated=%d rpcErr=%d dbErr=%d elapsed=%s",
		rangesDone.Load(), tracesTotal.Load(), creates.Load(), updated.Load(),
		rpcErrors.Load(), dbErrors.Load(), elapsed)

	if rpcErrors.Load() > 0 || dbErrors.Load() > 0 {
		log.Printf("ERRORS DETECTED — re-run to retry (idempotent; use same --from/--to)")
	} else {
		log.Printf("OK — no errors")
	}
}

// ── trace_filter call ─────────────────────────────────────────────────────────

type updateWork struct {
	address  string
	blockNum int64
	deployer string
	txHash   string
}

// processRange calls trace_filter for [from, to] and emits create-type traces to updateCh.
// If the node returns "Response is too big", the range is split in half recursively
// (down to individual blocks). This handles DoS-era blocks (~2.2M-2.7M) that have
// thousands of traces per block.
func processRange(from, to int64, updateCh chan<- updateWork) {
	defer rangesDone.Add(1)
	processRangeInner(from, to, updateCh)
}

func processRangeInner(from, to int64, updateCh chan<- updateWork) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "trace_filter",
		"params": []interface{}{
			map[string]interface{}{
				"fromBlock": fmt.Sprintf("0x%x", from),
				"toBlock":   fmt.Sprintf("0x%x", to),
			},
		},
		"id": 1,
	})

	var respBody []byte
	var lastErr string
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			if backoff > 16*time.Second {
				backoff = 16 * time.Second
			}
			time.Sleep(backoff)
		}
		resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err.Error()
			continue
		}
		respBody, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			lastErr = fmt.Sprintf("HTTP %d", resp.StatusCode)
			continue
		}
		lastErr = ""
		break
	}
	if lastErr != "" {
		rpcErrors.Add(1)
		log.Printf("[rpc-err] trace_filter %d→%d: %s", from, to, lastErr)
		return
	}

	var result struct {
		Result []traceEntry `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		rpcErrors.Add(1)
		log.Printf("[rpc-err] decode %d→%d: %v (body=%q)", from, to, err, truncate(respBody, 200))
		return
	}
	if result.Error != nil {
		msg := result.Error.Message
		// "Response is too big" — split range in half and retry each half.
		if strings.Contains(msg, "too big") || strings.Contains(msg, "too large") ||
			strings.Contains(msg, "Too Big") || strings.Contains(msg, "oversized") {
			if from == to {
				// Can't split further (single block); skip and log.
				rpcErrors.Add(1)
				log.Printf("[rpc-err] trace_filter block=%d: response too big even for 1 block (skipping)", from)
				return
			}
			mid := (from + to) / 2
			log.Printf("[split] range %d→%d too big, splitting at %d", from, to, mid)
			processRangeInner(from, mid, updateCh)
			processRangeInner(mid+1, to, updateCh)
			return
		}
		rpcErrors.Add(1)
		log.Printf("[rpc-err] trace_filter %d→%d: %s", from, to, msg)
		return
	}

	tracesTotal.Add(int64(len(result.Result)))

	for _, t := range result.Result {
		if t.Type != "create" {
			continue
		}
		// Skip reverted creates (error field non-empty means the contract wasn't deployed).
		if t.Error != "" {
			continue
		}
		addr := t.Result.Address
		deployer := t.Action.From
		txHash := t.TransactionHash
		if addr == "" || deployer == "" {
			continue
		}
		creates.Add(1)
		updateCh <- updateWork{
			address:  addr,
			blockNum: t.BlockNumber,
			deployer: deployer,
			txHash:   txHash,
		}
	}
}

// ── DB update ─────────────────────────────────────────────────────────────────

func doUpdate(w updateWork) {
	err := withRetry(func() error {
		return session.Query(updDeployerQ, w.deployer, w.txHash, w.address, w.blockNum).Exec()
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

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
