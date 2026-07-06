// backfill_deployer_targeted: two-phase targeted deployer backfill.
//
// Phase 1 (~40 min): full paginated scan of contracts_by_address_v2 for rows
// where deployer is null/empty. Builds: (address,block_number) null set and
// map of unique 100-block chunk starts that contain at least one null row.
//
// Phase 2: calls trace_filter only for chunks that have null rows (~2-4x faster
// than exhaustive deployer_main). Updates deployer+tx_hash via idempotent UPDATE.
//
// Safe to run in parallel with deployer_main — both do idempotent UPDATEs.
// Point --rpc at eth07 (not eth60) to avoid competing with deployer_main.
//
// Usage: backfill_deployer_targeted --host 127.0.0.1 --pass cassandra [flags]
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

type rowKey struct {
	address     string
	blockNumber int64
}

type updateWork struct {
	address     string
	blockNumber int64
	deployer    string
	txHash      string
}

type traceAction struct {
	From string `json:"from"`
}
type traceResult struct {
	Address string `json:"address"`
}
type traceEntry struct {
	Type            string      `json:"type"`
	Action          traceAction `json:"action"`
	Result          traceResult `json:"result"`
	TransactionHash string      `json:"transactionHash"`
	BlockNumber     int64       `json:"blockNumber"`
	Error           string      `json:"error"`
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
	rpc        := flag.String("rpc", "http://100.64.0.7:8545", "ETH RPC URL (default: eth07)")
	rpcWorkers := flag.Int("rpc-workers", 30, "concurrent trace_filter workers")
	dbWorkers  := flag.Int("db-workers", 64, "concurrent DB UPDATE workers")
	pageSize   := flag.Int("page-size", 5000, "scan page size")
	dryRun     := flag.Bool("dry-run", false, "scan+collect only, don't write to DB")
	flag.Parse()

	rpcURL = *rpc
	httpClient = &http.Client{Timeout: 120 * time.Second}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalQuorum
	cluster.NumConns = 8
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// ── Phase 1: collect null rows ──────────────────────────────────────────
	log.Printf("=== Phase 1: scanning contracts_by_address_v2 for null deployer rows ===")
	p1Start := time.Now()

	nullSet := make(map[rowKey]bool, 400_000)
	chunkSet := make(map[int64]bool, 8_000)

	var p1Scanned, p1Nulls atomic.Int64

	stopP1 := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopP1:
				return
			case <-time.After(30 * time.Second):
				sc := p1Scanned.Load()
				nl := p1Nulls.Load()
				elapsed := time.Since(p1Start)
				rate := float64(sc) / elapsed.Seconds()
				log.Printf("[phase1] scanned=%d nulls=%d uniqueChunks=%d rate=%.0f rows/s elapsed=%s",
					sc, nl, len(chunkSet), rate, elapsed.Round(time.Second))
			}
		}
	}()

	const sel = `SELECT address, block_number, deployer FROM eth.contracts_by_address_v2`
	iter := session.Query(sel).PageSize(*pageSize).Iter()

	var (
		addr     string
		blkNum   int64
		deployer string
	)
	for iter.Scan(&addr, &blkNum, &deployer) {
		p1Scanned.Add(1)
		if deployer == "" {
			p1Nulls.Add(1)
			addrLow := strings.ToLower(addr)
			key := rowKey{addrLow, blkNum}
			nullSet[key] = true
			chunkStart := (blkNum / 100) * 100
			chunkSet[chunkStart] = true
		}
		deployer = "" // reset for next iteration (gocql reuses)
	}
	if err := iter.Close(); err != nil {
		log.Printf("[phase1] iter error: %v", err)
	}
	close(stopP1)

	p1Elapsed := time.Since(p1Start).Round(time.Second)
	p1Sc := p1Scanned.Load()
	p1Nl := p1Nulls.Load()
	totalChunks := int64(len(chunkSet))
	log.Printf("=== Phase 1 done: scanned=%d nulls=%d uniqueChunks=%d elapsed=%s ===",
		p1Sc, p1Nl, totalChunks, p1Elapsed)

	if p1Nl == 0 {
		log.Println("No null deployer rows found. Nothing to do.")
		fmt.Println("=== DONE === 0 nulls")
		return
	}

	// ── Phase 2: trace_filter for null chunks only ─────────────────────────
	log.Printf("=== Phase 2: processing %d chunks (rpc-workers=%d, db-workers=%d, dry-run=%v) ===",
		totalChunks, *rpcWorkers, *dbWorkers, *dryRun)
	p2Start := time.Now()

	chunks := make([]int64, 0, totalChunks)
	for start := range chunkSet {
		chunks = append(chunks, start)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })

	// DB workers
	updateCh := make(chan updateWork, *dbWorkers*4)
	var wgDB sync.WaitGroup
	var p2Updated, p2DbErr atomic.Int64

	const updQ = `UPDATE eth.contracts_by_address_v2 SET deployer = ?, tx_hash = ? WHERE address = ? AND block_number = ?`
	for i := 0; i < *dbWorkers; i++ {
		wgDB.Add(1)
		go func() {
			defer wgDB.Done()
			for uw := range updateCh {
				if *dryRun {
					p2Updated.Add(1)
					continue
				}
				if err := retryDB(func() error {
					return session.Query(updQ, uw.deployer, uw.txHash, uw.address, uw.blockNumber).Exec()
				}); err != nil {
					p2DbErr.Add(1)
					log.Printf("[err] update addr=%s blk=%d: %v", uw.address, uw.blockNumber, err)
				} else {
					p2Updated.Add(1)
				}
			}
		}()
	}

	// RPC workers
	chunkCh := make(chan int64, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var p2ChunksDone, p2Creates atomic.Int64

	// Phase 2 progress
	go func() {
		for {
			time.Sleep(30 * time.Second)
			done := p2ChunksDone.Load()
			cr := p2Creates.Load()
			up := p2Updated.Load()
			de := p2DbErr.Load()
			elapsed := time.Since(p2Start)
			rate := float64(done) / elapsed.Seconds() * 100
			eta := ""
			if done > 0 && done < totalChunks {
				rem := float64(totalChunks-done) / (float64(done) / elapsed.Seconds())
				eta = fmt.Sprintf(" eta=%s", (time.Duration(rem * float64(time.Second))).Round(time.Second))
			}
			log.Printf("[phase2] chunks=%d/%d creates=%d updated=%d dbErr=%d rate=%.0f blk/s elapsed=%s%s",
				done, totalChunks, cr, up, de, rate, elapsed.Round(time.Second), eta)
		}
	}()

	var wgRPC sync.WaitGroup
	for i := 0; i < *rpcWorkers; i++ {
		wgRPC.Add(1)
		go func() {
			defer wgRPC.Done()
			for chunkStart := range chunkCh {
				processRangeInner(chunkStart, chunkStart+99, nullSet, updateCh, &p2Creates)
				p2ChunksDone.Add(1)
			}
		}()
	}

	wgRPC.Wait()
	close(updateCh)
	wgDB.Wait()

	up := p2Updated.Load()
	de := p2DbErr.Load()
	cr := p2Creates.Load()
	p2Elapsed := time.Since(p2Start).Round(time.Second)

	fmt.Println("=== DONE ===")
	fmt.Printf("phase1: scanned=%d  nulls=%d  uniqueChunks=%d  elapsed=%s\n", p1Sc, p1Nl, totalChunks, p1Elapsed)
	fmt.Printf("phase2: creates=%d  updated=%d  dbErr=%d  elapsed=%s\n", cr, up, de, p2Elapsed)
	if de > 0 {
		fmt.Printf("WARNING: %d rows failed to update — re-run to retry\n", de)
	} else {
		fmt.Println("OK — no errors")
	}
}

// processRangeInner calls trace_filter for [from,to] and sends matching null
// rows to updateCh. Recursively halves the range on "response too big" errors.
func processRangeInner(from, to int64, nullSet map[rowKey]bool, updateCh chan<- updateWork, creates *atomic.Int64) {
	fromHex := fmt.Sprintf("0x%x", from)
	toHex := fmt.Sprintf("0x%x", to)
	body := fmt.Sprintf(`{"jsonrpc":"2.0","method":"trace_filter","params":[{"fromBlock":%q,"toBlock":%q}],"id":1}`,
		fromHex, toHex)

	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := callRPC(body)
		if err != nil {
			if attempt == 3 {
				log.Printf("[rpc-err] range [%d,%d] attempt %d: %v", from, to, attempt, err)
				return
			}
			time.Sleep(delay)
			delay = minDur(delay*2, 30*time.Second)
			continue
		}
		if resp.Error != nil {
			msg := resp.Error.Message
			if strings.Contains(msg, "too big") || strings.Contains(msg, "Response is too big") ||
				strings.Contains(msg, "too large") || strings.Contains(msg, "limit exceeded") {
				if to-from <= 1 {
					log.Printf("[rpc-err] range [%d,%d] too big and can't split further", from, to)
					return
				}
				mid := (from + to) / 2
				processRangeInner(from, mid, nullSet, updateCh, creates)
				processRangeInner(mid+1, to, nullSet, updateCh, creates)
				return
			}
			if attempt == 3 {
				log.Printf("[rpc-err] range [%d,%d]: %s", from, to, msg)
				return
			}
			time.Sleep(delay)
			delay = minDur(delay*2, 30*time.Second)
			continue
		}

		// Match trace results against nullSet
		for _, t := range resp.Result {
			if t.Type != "create" || t.Error != "" || t.Result.Address == "" {
				continue
			}
			contractAddr := strings.ToLower(t.Result.Address)
			key := rowKey{contractAddr, t.BlockNumber}
			if nullSet[key] {
				creates.Add(1)
				updateCh <- updateWork{
					address:     contractAddr,
					blockNumber: t.BlockNumber,
					deployer:    strings.ToLower(t.Action.From),
					txHash:      t.TransactionHash,
				}
			}
		}
		return
	}
}

func callRPC(body string) (*rpcResp, error) {
	req, err := http.NewRequest("POST", rpcURL, bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
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
