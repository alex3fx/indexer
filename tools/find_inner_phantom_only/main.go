// find_inner_phantom_only: identifies which inner-phantom addresses (from check_inner_phantom output)
// have NO real deployment at block <= block-hi.
//
// "Inner-phantom": outer tx.status=1, but inner CREATE call was reverted by parent REVERT.
// Erigon records result.address for the CREATE frame even though no contract was deployed.
// Dune counts only trace-level success=true; we stored these addresses → inflate COUNT DISTINCT.
//
// Algorithm:
//  1. Load inner_phantom_in_scylla.txt (format: address\trowCount)
//  2. For each address, query Scylla for ALL block_numbers at block <= block-hi
//  3. For each block_number, call eth_getCode(address, block_number+1) via archive RPC
//     - block <= rpc-split → rpc-lo (default: .7, covers 0..12_700_000)
//     - block > rpc-split  → rpc-hi (default: .60, covers 12_700_001..HEAD)
//  4. If ANY call returns non-empty code → real deployment → skip
//  5. If all code calls return "0x" or "" → inner-phantom-only → write to output
//
// Usage:
//   ./find_inner_phantom_only \
//     --pass=cassandra \
//     --addr-file=inner_phantom_in_scylla.txt \
//     --block-hi=25422000 \
//     --rpc-lo=http://100.64.0.7:8545 \
//     --rpc-hi=http://100.64.0.60:8545 \
//     --rpc-split=12700000
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

// ── JSON-RPC helpers ──────────────────────────────────────────────────────────

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

type rpcResp struct {
	Result string `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	},
}

func ethGetCode(rpcURL, addr string, blockNum int64) (string, error) {
	blockHex := fmt.Sprintf("0x%x", blockNum+1)
	req := rpcReq{JSONRPC: "2.0", Method: "eth_getCode", Params: []any{addr, blockHex}, ID: 1}
	body, _ := json.Marshal(req)

	var lastErr error
	backoff := 300 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}
		resp, err := httpClient.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		var r rpcResp
		decErr := json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if decErr != nil {
			lastErr = decErr
			continue
		}
		if r.Error != nil {
			lastErr = fmt.Errorf("rpc error %d: %s", r.Error.Code, r.Error.Message)
			continue
		}
		return r.Result, nil
	}
	return "", lastErr
}

// ── Scylla helpers ────────────────────────────────────────────────────────────

// getBlockNumbers returns all block_numbers for addr at block <= blockHi.
func getBlockNumbers(session *gocql.Session, addr string, blockHi int64) ([]int64, error) {
	var blocks []int64
	backoff := 300 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}
		iter := session.Query(
			`SELECT block_number FROM eth.contracts_by_address_v2
			 WHERE address = ? AND block_number <= ?
			 ALLOW FILTERING`,
			addr, blockHi,
		).PageSize(50).Iter()
		blocks = blocks[:0]
		var bn int64
		for iter.Scan(&bn) {
			blocks = append(blocks, bn)
		}
		if err := iter.Close(); err != nil {
			lastErr = err
			blocks = nil
			continue
		}
		return blocks, nil
	}
	return nil, lastErr
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "Scylla user")
	pass     := flag.String("pass", "", "Scylla password")
	addrFile := flag.String("addr-file", "inner_phantom_in_scylla.txt", "input: check_inner_phantom output (address\\trowCount)")
	blockHi  := flag.Int64("block-hi", 25422000, "historical range upper bound (inclusive)")
	rpcLo    := flag.String("rpc-lo", "http://100.64.0.7:8545", "RPC for blocks [0, rpc-split]")
	rpcHi    := flag.String("rpc-hi", "http://100.64.0.60:8545", "RPC for blocks (rpc-split, HEAD]")
	rpcSplit := flag.Int64("rpc-split", 12700000, "block number boundary between rpc-lo and rpc-hi")
	workers  := flag.Int("workers", 32, "parallel workers")
	outFile  := flag.String("out", "inner_phantom_only.txt", "output: inner-phantom-only addresses")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	// Load addresses
	addrs, err := loadAddresses(*addrFile)
	if err != nil {
		log.Fatalf("load addr-file: %v", err)
	}
	log.Printf("Loaded %d addresses from %s", len(addrs), *addrFile)

	// Connect to Scylla
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = "eth"
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect Scylla: %v", err)
	}
	defer session.Close()

	// Open output
	out, err := os.Create(*outFile)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer out.Close()
	outBuf := bufio.NewWriter(out)
	var outMu sync.Mutex

	// Counters
	var (
		checked        atomic.Int64
		phantomOnly    atomic.Int64
		hasRealDeploy  atomic.Int64
		scyllaErrors   atomic.Int64
		rpcErrors      atomic.Int64
	)

	total := len(addrs)
	start := time.Now()

	// Progress reporter
	go func() {
		for {
			time.Sleep(15 * time.Second)
			c := checked.Load()
			elapsed := time.Since(start).Seconds()
			var rate, eta float64
			if elapsed > 0 {
				rate = float64(c) / elapsed
				if rate > 0 {
					eta = float64(total-int(c)) / rate
				}
			}
			log.Printf("[progress] checked=%d/%d phantom-only=%d real=%d scylla-err=%d rpc-err=%d rate=%.0f/s eta=%.0fs",
				c, total, phantomOnly.Load(), hasRealDeploy.Load(),
				scyllaErrors.Load(), rpcErrors.Load(), rate, eta)
		}
	}()

	// Worker pool
	addrCh := make(chan string, 200)
	go func() {
		for _, a := range addrs {
			addrCh <- a
		}
		close(addrCh)
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range addrCh {
				isPhantomOnly, err := checkAddress(session, addr, *blockHi, *rpcLo, *rpcHi, *rpcSplit)
				if err != nil {
					if strings.Contains(err.Error(), "rpc") {
						rpcErrors.Add(1)
					} else {
						scyllaErrors.Add(1)
					}
					log.Printf("[ERROR] %s: %v", addr, err)
				} else if isPhantomOnly {
					phantomOnly.Add(1)
					outMu.Lock()
					fmt.Fprintln(outBuf, addr)
					outMu.Unlock()
				} else {
					hasRealDeploy.Add(1)
				}
				checked.Add(1)
			}
		}()
	}

	wg.Wait()
	outBuf.Flush()

	elapsed := time.Since(start)
	fmt.Println("\n=== RESULT ===")
	fmt.Printf("addr-file:       %s\n", *addrFile)
	fmt.Printf("block-hi:        %d\n", *blockHi)
	fmt.Printf("total checked:   %d\n", checked.Load())
	fmt.Printf("inner-phantom-only (no real deploy): %d\n", phantomOnly.Load())
	fmt.Printf("has real deploy: %d\n", hasRealDeploy.Load())
	fmt.Printf("scylla errors:   %d\n", scyllaErrors.Load())
	fmt.Printf("rpc errors:      %d\n", rpcErrors.Load())
	fmt.Printf("elapsed:         %s\n", elapsed.Round(time.Second))
	fmt.Printf("output:          %s\n", *outFile)
}

// checkAddress returns true if the address has NO real deployment at block <= blockHi.
// Queries Scylla for all block_numbers, then calls eth_getCode at each block+1.
func checkAddress(
	session *gocql.Session,
	addr string, blockHi int64,
	rpcLo, rpcHi string, rpcSplit int64,
) (bool, error) {
	blocks, err := getBlockNumbers(session, addr, blockHi)
	if err != nil {
		return false, fmt.Errorf("scylla: %w", err)
	}
	if len(blocks) == 0 {
		// Row was deleted between check_inner_phantom scan and now — treat as resolved.
		return false, nil
	}

	for _, bn := range blocks {
		rpc := rpcHi
		if bn <= rpcSplit {
			rpc = rpcLo
		}
		code, err := ethGetCode(rpc, addr, bn)
		if err != nil {
			return false, fmt.Errorf("rpc: %w", err)
		}
		if code != "" && code != "0x" {
			// Non-empty bytecode → real deployment at this block.
			return false, nil
		}
	}

	// All blocks gave empty code → inner-phantom-only.
	return true, nil
}

func loadAddresses(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var addrs []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// Format: address\trowCount  (tab-separated, first field is address)
		addr := strings.SplitN(line, "\t", 2)[0]
		addr = strings.ToLower(addr)
		if !strings.HasPrefix(addr, "0x") {
			addr = "0x" + addr
		}
		addrs = append(addrs, addr)
	}
	return addrs, sc.Err()
}
