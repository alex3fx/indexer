// check_tx_status: sample contracts from contracts_by_address_v2 across token ranges,
// look up the tx status from the transactions table, and count how many contracts
// come from failed (status=0) transactions. Tests hypothesis that indexer records
// contracts from reverted traces/transactions that Etherscan excludes.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

func chunkForBlock(block int64) int32 {
	lane := block % 24
	era := block / 12000
	return int32(lane + 24*era)
}

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "user")
	pass     := flag.String("pass", "", "password")
	segments := flag.Int("segments", 64, "token-range segments to scan")
	perSeg   := flag.Int("per-seg", 200, "contracts to sample per segment")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 8
	cluster.Timeout = 120 * time.Second
	cluster.ConnectTimeout = 15 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	n := *segments
	totalF := float64(uint64(math.MaxUint64)) + 1.0
	stepF := totalF / float64(n)

	type result struct {
		total   int64
		failed  int64 // tx status=0
		success int64 // tx status=1
		noTx    int64 // tx not found in transactions table
		errors  int64
	}
	var res result

	var wg sync.WaitGroup
	var mu sync.Mutex
	failedExamples := make([]string, 0, 10)

	for i := 0; i < n; i++ {
		lo := int64(math.MinInt64 + int64(float64(i)*stepF))
		var hi int64
		if i == n-1 {
			hi = math.MaxInt64
		} else {
			hi = int64(math.MinInt64 + int64(float64(i+1)*stepF))
		}

		wg.Add(1)
		go func(lo, hi int64) {
			defer wg.Done()

			type contract struct {
				address     string
				txHash      string
				blockNumber int64
			}

			// Sample contracts from this token range.
			var contracts []contract
			iter := session.Query(
				`SELECT address, tx_hash, block_number FROM eth.contracts_by_address_v2
				 WHERE token(address) >= ? AND token(address) < ?`,
				lo, hi,
			).PageSize(*perSeg).Iter()

			var addr, txHash string
			var block int64
			for iter.Scan(&addr, &txHash, &block) {
				if txHash == "" {
					continue // ghost row, skip
				}
				contracts = append(contracts, contract{addr, txHash, block})
				if len(contracts) >= *perSeg {
					break
				}
			}
			iter.Close()

			// Look up tx status for each sampled contract.
			var local result
			for _, c := range contracts {
				chunk := chunkForBlock(c.blockNumber)
				var status int8
				var foundHash string
				scanIter := session.Query(
					`SELECT hash, status FROM eth.transactions
					 WHERE chunk=? AND block_number=?`,
					chunk, c.blockNumber,
				).PageSize(500).Iter()
				found := false
				for scanIter.Scan(&foundHash, &status) {
					if foundHash == c.txHash {
						found = true
						break
					}
				}
				scanIter.Close()

				local.total++
				if !found {
					local.noTx++
				} else if status == 0 {
					local.failed++
					mu.Lock()
					if len(failedExamples) < 10 {
						failedExamples = append(failedExamples,
							fmt.Sprintf("addr=%s block=%d tx=%s", c.address, c.blockNumber, c.txHash))
					}
					mu.Unlock()
				} else {
					local.success++
				}
			}

			atomic.AddInt64(&res.total, local.total)
			atomic.AddInt64(&res.failed, local.failed)
			atomic.AddInt64(&res.success, local.success)
			atomic.AddInt64(&res.noTx, local.noTx)
			atomic.AddInt64(&res.errors, local.errors)
		}(lo, hi)
	}

	wg.Wait()

	fmt.Println("=== RESULT ===")
	fmt.Printf("sampled contracts: %d\n", res.total)
	fmt.Printf("  tx status=1 (success): %d (%.2f%%)\n", res.success, 100*float64(res.success)/float64(res.total))
	fmt.Printf("  tx status=0 (FAILED):  %d (%.2f%%)\n", res.failed, 100*float64(res.failed)/float64(res.total))
	fmt.Printf("  tx not found:          %d (%.2f%%)\n", res.noTx, 100*float64(res.noTx)/float64(res.total))
	if len(failedExamples) > 0 {
		fmt.Println("\nFailed tx examples:")
		for _, ex := range failedExamples {
			fmt.Println(" ", ex)
		}
	}
}
