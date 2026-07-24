// verify_inner_phantom: checks that inner-phantom contract addresses (CREATE traces
// whose ancestor sub-call was reverted) are NOT present in contracts_by_address_v2.
//
// Strategy:
//   1. For each block in [from, to], call trace_block on the local Erigon node.
//   2. Build a set of "reverted ancestors": traces that have error != "" and a non-empty traceAddress,
//      GROUPED BY transactionPosition (critical: cross-tx matching is a false-positive bug).
//   3. For each CREATE trace with result.address (no error), check if any reverted ancestor
//      in THE SAME TRANSACTION is a prefix of its traceAddress → inner-phantom.
//   4. Query Scylla contracts_by_address_v2 for each inner-phantom address.
//      Any hit means the transformer fix is NOT working.
//
// Expected result after v27 deployment: 0 inner-phantom addresses in Scylla for the scanned range.
// NOTE: v26 had a cross-tx matching bug (reverted entries not bound to txPos) causing false positives.
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
	"time"

	"github.com/gocql/gocql"
)

// ── RPC types ────────────────────────────────────────────────────────────────

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

type traceAction struct {
	CallType string `json:"callType"`
	To       string `json:"to"`
	Init     string `json:"init"` // present on create
}

type traceResult struct {
	Address string `json:"address"` // present on create
	Code    string `json:"code"`
}

type traceEntry struct {
	Action              traceAction `json:"action"`
	Result              traceResult `json:"result"`
	Error               string      `json:"error"`
	TraceAddress        []int       `json:"traceAddress"`
	TransactionPosition *int        `json:"transactionPosition"`
	Type                string      `json:"type"`
}

type traceBlockResp struct {
	Result []traceEntry `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// ── RPC call ─────────────────────────────────────────────────────────────────

func traceBlock(rpcURL string, blockNum int64) ([]traceEntry, error) {
	blockHex := fmt.Sprintf("0x%x", blockNum)
	reqBody, _ := json.Marshal(rpcReq{
		JSONRPC: "2.0", Method: "trace_block",
		Params: []any{blockHex}, ID: 1,
	})
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	var r traceBlockResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("json: %w (body=%s)", err, truncate(string(body), 200))
	}
	if r.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", r.Error.Message)
	}
	return r.Result, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ── Inner-phantom detection ───────────────────────────────────────────────────

// isPrefix returns true if prefix is a strict prefix of full (i.e. an ancestor trace).
func isPrefix(prefix, full []int) bool {
	if len(prefix) == 0 || len(prefix) >= len(full) {
		return false
	}
	for i, v := range prefix {
		if full[i] != v {
			return false
		}
	}
	return true
}

type revertedEntry struct {
	txPos int
	addr  []int
}

type innerPhantom struct {
	BlockNum   int64
	Address    string
	TraceAddr  []int
	RevertedAt []int
}

func findInnerPhantoms(blockNum int64, traces []traceEntry) []innerPhantom {
	// collect reverted sub-call traceAddresses, bound to their transaction position
	var reverted []revertedEntry
	for _, t := range traces {
		if t.Error != "" && len(t.TraceAddress) > 0 && t.TransactionPosition != nil {
			cp := make([]int, len(t.TraceAddress))
			copy(cp, t.TraceAddress)
			reverted = append(reverted, revertedEntry{txPos: *t.TransactionPosition, addr: cp})
		}
	}
	if len(reverted) == 0 {
		return nil
	}

	var out []innerPhantom
	for _, t := range traces {
		if t.Type != "create" {
			continue
		}
		if t.Error != "" {
			continue // failed itself — no result.address
		}
		addr := strings.ToLower(t.Result.Address)
		if addr == "" || addr == "0x" {
			continue
		}
		if t.TransactionPosition == nil {
			continue
		}
		traceTxPos := *t.TransactionPosition
		// check if any reverted ancestor IN THE SAME TRANSACTION is a prefix
		for _, rev := range reverted {
			if rev.txPos == traceTxPos && isPrefix(rev.addr, t.TraceAddress) {
				cp := make([]int, len(t.TraceAddress))
				copy(cp, t.TraceAddress)
				out = append(out, innerPhantom{
					BlockNum:   blockNum,
					Address:    addr,
					TraceAddr:  cp,
					RevertedAt: rev.addr,
				})
				break
			}
		}
	}
	return out
}

// ── Scylla check ─────────────────────────────────────────────────────────────

func checkScylla(session *gocql.Session, addr string, blockNum int64) (bool, string, error) {
	var txHash string
	err := session.Query(
		`SELECT tx_hash FROM contracts_by_address_v2 WHERE address=? AND block_number=?`,
		addr, blockNum,
	).Scan(&txHash)
	if err == gocql.ErrNotFound {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, txHash, nil
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	rpcURL   := flag.String("rpc", "http://100.64.0.60:8545", "Erigon RPC URL")
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "Scylla user")
	pass     := flag.String("pass", "", "Scylla password")
	keyspace := flag.String("keyspace", "eth", "Scylla keyspace")
	from     := flag.Int64("from", 25600296, "first block to check (first v26-era block)")
	to       := flag.Int64("to", 25600400, "last block to check")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("scylla connect: %v", err)
	}
	defer session.Close()

	log.Printf("Scanning blocks [%d, %d] for inner-phantom creates...", *from, *to)

	var (
		blocksScanned    int
		phantomsFound    int
		phantomsInScylla int
		phantomsAbsent   int
		rpcErrors        int
		scyllaErrors     int
	)

	for bn := *from; bn <= *to; bn++ {
		traces, err := traceBlock(*rpcURL, bn)
		if err != nil {
			log.Printf("[block %d] trace_block error: %v", bn, err)
			rpcErrors++
			time.Sleep(500 * time.Millisecond)
			continue
		}
		blocksScanned++

		phantoms := findInnerPhantoms(bn, traces)
		if len(phantoms) > 0 {
			log.Printf("[block %d] found %d inner-phantom(s)", bn, len(phantoms))
		}

		for _, p := range phantoms {
			phantomsFound++
			found, txHash, err := checkScylla(session, p.Address, p.BlockNum)
			if err != nil {
				log.Printf("  [scylla error] addr=%s block=%d: %v", p.Address, p.BlockNum, err)
				scyllaErrors++
				continue
			}
			if found {
				phantomsInScylla++
				fmt.Printf("[BUG] inner-phantom IN DB: addr=%s block=%d tx=%s traceAddr=%v revertedAt=%v\n",
					p.Address, p.BlockNum, txHash, p.TraceAddr, p.RevertedAt)
			} else {
				phantomsAbsent++
				fmt.Printf("[OK ] inner-phantom absent: addr=%s block=%d traceAddr=%v revertedAt=%v\n",
					p.Address, p.BlockNum, p.TraceAddr, p.RevertedAt)
			}
		}

		// small delay to avoid hammering RPC
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Println()
	fmt.Println("=== RESULT ===")
	fmt.Printf("blocks scanned:            %d\n", blocksScanned)
	fmt.Printf("inner-phantom creates:     %d\n", phantomsFound)
	fmt.Printf("  correct (absent in DB):  %d\n", phantomsAbsent)
	fmt.Printf("  BUG (present in DB):     %d\n", phantomsInScylla)
	fmt.Printf("rpc errors:                %d\n", rpcErrors)
	fmt.Printf("scylla errors:             %d\n", scyllaErrors)

	if phantomsInScylla == 0 && phantomsFound > 0 {
		fmt.Println()
		fmt.Println("✓ PASS: inner-phantom fix is working correctly")
	} else if phantomsInScylla > 0 {
		fmt.Println()
		fmt.Printf("✗ FAIL: %d inner-phantom addresses found in DB — fix not working\n", phantomsInScylla)
	} else if phantomsFound == 0 {
		fmt.Println()
		fmt.Println("? NO PHANTOMS FOUND in this range — try a wider range or different blocks")
	}
}
