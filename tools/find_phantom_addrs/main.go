// find_phantom_addrs: scans all rows in contracts_by_address_v2, looks up tx status for
// each deployment event from our own transactions table, and outputs phantom rows.
//
// A row is phantom if its specific tx_hash has tx.status=0 (failed tx; contract never
// existed on mainnet). An address may have BOTH phantom rows (failed attempts) and real
// rows (successful deployments) — e.g. CREATE2 retry after a failed tx. Both cases are
// handled: phantom rows are always emitted even if the same address also has real rows.
//
// Classification written to --out file:
//   address\tphantom\tblock_number\ttx_hash  — phantom row
//   address\tghost-only                       — all rows for address have tx_hash=null
//
// No external dependencies — uses only our Scylla database.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
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
	segments := flag.Int("segments", 256, "token-range segments")
	pageSize := flag.Int("page-size", 1000, "CQL page size per segment")
	outFile  := flag.String("out", "phantom_addresses.txt", "output file for phantom/ghost addresses")
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

	f, err := os.Create(*outFile)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()

	var (
		totalAddrs   atomic.Int64
		realAddrs    atomic.Int64
		phantomAddrs atomic.Int64
		ghostOnly    atomic.Int64
		txErrors     atomic.Int64
	)
	var writeMu sync.Mutex

	n := *segments
	totalF := float64(uint64(math.MaxUint64)) + 1.0
	stepF := totalF / float64(n)

	start := time.Now()
	log.Printf("Starting phantom scan: segments=%d page-size=%d out=%s", n, *pageSize, *outFile)

	go func() {
		for {
			time.Sleep(30 * time.Second)
			ta := totalAddrs.Load()
			elapsed := time.Since(start)
			rate := float64(ta) / elapsed.Seconds()
			log.Printf("[progress] addrs=%d real=%d phantom=%d ghost-only=%d tx-errors=%d rate=%.0f/s elapsed=%s",
				ta, realAddrs.Load(), phantomAddrs.Load(), ghostOnly.Load(), txErrors.Load(),
				rate, elapsed.Round(time.Second))
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		lo := int64(math.MinInt64 + int64(float64(i)*stepF))
		last := i == n-1
		var hi int64
		if last {
			hi = math.MaxInt64
		} else {
			hi = int64(math.MinInt64 + int64(float64(i+1)*stepF))
		}

		wg.Add(1)
		go func(lo, hi int64, last bool) {
			defer wg.Done()
			processSegment(session, lo, hi, last, *pageSize,
				&totalAddrs, &realAddrs, &phantomAddrs, &ghostOnly, &txErrors,
				f, &writeMu)
		}(lo, hi, last)
	}

	wg.Wait()

	elapsed := time.Since(start).Round(time.Second)
	ta := totalAddrs.Load()
	ra := realAddrs.Load()
	pa := phantomAddrs.Load()
	go_ := ghostOnly.Load()
	te := txErrors.Load()

	fmt.Println("=== RESULT ===")
	fmt.Printf("total unique addresses:     %d\n", ta)
	fmt.Printf("real (≥1 tx with status=1): %d\n", ra)
	fmt.Printf("phantom (all tx status=0):  %d\n", pa)
	fmt.Printf("ghost-only (no tx_hash):    %d\n", go_)
	fmt.Printf("tx lookup errors:           %d\n", te)
	fmt.Printf("elapsed:                    %s\n", elapsed)
	fmt.Printf("phantom+ghost written to:   %s\n", *outFile)
	if te > 0 {
		fmt.Println("WARNING: tx lookup errors occurred — phantom count may be underestimated (errors treated as real)")
	}
}

type deployRow struct {
	txHash  string
	blockN  int64
	txIndex int // transaction_index in the block; -1 if null
}

func processSegment(
	session *gocql.Session,
	lo, hi int64, last bool, pageSize int,
	totalAddrs, realAddrs, phantomAddrs, ghostOnly, txErrors *atomic.Int64,
	f *os.File, writeMu *sync.Mutex,
) {
	var q *gocql.Query
	if last {
		q = session.Query(
			`SELECT address, tx_hash, block_number, transaction_index
			 FROM eth.contracts_by_address_v2
			 WHERE token(address) >= ?`,
			lo,
		)
	} else {
		q = session.Query(
			`SELECT address, tx_hash, block_number, transaction_index
			 FROM eth.contracts_by_address_v2
			 WHERE token(address) >= ? AND token(address) < ?`,
			lo, hi,
		)
	}

	iter := q.PageSize(pageSize).Iter()

	var (
		curAddr string
		curRows []deployRow
	)

	flush := func() {
		if curAddr == "" || len(curRows) == 0 {
			return
		}
		totalAddrs.Add(1)
		classifyAddress(session, curAddr, curRows,
			realAddrs, phantomAddrs, ghostOnly, txErrors,
			f, writeMu)
		curRows = curRows[:0]
	}

	var addr, txHash string
	var blockN int64
	var txIndexPtr *int // nullable int

	for iter.Scan(&addr, &txHash, &blockN, &txIndexPtr) {
		if addr != curAddr {
			flush()
			curAddr = addr
		}
		txIdx := -1
		if txIndexPtr != nil {
			txIdx = *txIndexPtr
		}
		curRows = append(curRows, deployRow{
			txHash:  txHash,
			blockN:  blockN,
			txIndex: txIdx,
		})
		// reset for next iteration
		txHash = ""
		txIndexPtr = nil
	}
	flush()

	if err := iter.Close(); err != nil {
		log.Printf("[err] segment lo=%d: %v", lo, err)
	}
}

func classifyAddress(
	session *gocql.Session,
	addr string, rows []deployRow,
	realAddrs, phantomAddrs, ghostOnly, txErrors *atomic.Int64,
	f *os.File, writeMu *sync.Mutex,
) {
	// Ghost-only: all rows have tx_hash=""
	allGhost := true
	for _, r := range rows {
		if r.txHash != "" {
			allGhost = false
			break
		}
	}
	if allGhost {
		ghostOnly.Add(1)
		writeMu.Lock()
		fmt.Fprintf(f, "%s\tghost-only\n", addr)
		writeMu.Unlock()
		return
	}

	// Check each row independently. An address may have both phantom rows (status=0)
	// and real rows (status=1) — e.g. CREATE2 failed attempt then successful redeploy.
	// We must NOT stop at first status=1; keep checking all rows so phantom rows are found.
	hasReal := false
	var phantomRows []deployRow

	for _, r := range rows {
		if r.txHash == "" {
			continue // ghost row, skip
		}

		status, ok := lookupTxStatus(session, r.blockN, r.txIndex, r.txHash, txErrors)
		if !ok {
			// lookup error — treat this row as real to avoid false phantom classification
			hasReal = true
			continue // check remaining rows
		}
		if status == 1 {
			hasReal = true
			continue // check remaining rows for phantom rows
		}
		// status == 0: this specific row is a phantom
		phantomRows = append(phantomRows, r)
	}

	if hasReal {
		realAddrs.Add(1)
	} else {
		// All non-ghost rows have status=0 → pure phantom address
		phantomAddrs.Add(1)
	}

	// Write phantom rows regardless of whether the address also has real rows.
	// delete_phantom_ghost will delete only these specific (address, block_number) pairs.
	if len(phantomRows) > 0 {
		writeMu.Lock()
		for _, r := range phantomRows {
			fmt.Fprintf(f, "%s\tphantom\t%d\t%s\n", addr, r.blockN, r.txHash)
		}
		writeMu.Unlock()
	}
}

// lookupTxStatus returns (status, ok). ok=false on error (treat as real).
func lookupTxStatus(session *gocql.Session, blockN int64, txIndex int, txHash string, txErrors *atomic.Int64) (int8, bool) {
	chunk := chunkForBlock(blockN)

	if txIndex >= 0 {
		// Fast path: direct primary-key lookup using transaction_index.
		var status int8
		err := session.Query(
			`SELECT status FROM eth.transactions
			 WHERE chunk=? AND block_number=? AND transaction_index=?`,
			chunk, blockN, txIndex,
		).Scan(&status)
		if err == nil {
			return status, true
		}
		if err != gocql.ErrNotFound {
			txErrors.Add(1)
			return 0, false // error → caller treats as real
		}
		// ErrNotFound with known index: fall through to hash scan
		// (shouldn't happen often, but handle gracefully)
	}

	// Slow path: scan all transactions in the block and match by hash.
	// Used when transaction_index is null or not found by index.
	iter := session.Query(
		`SELECT hash, status FROM eth.transactions
		 WHERE chunk=? AND block_number=?`,
		chunk, blockN,
	).PageSize(500).Iter()

	var h string
	var s int8
	found := false
	for iter.Scan(&h, &s) {
		if h == txHash {
			found = true
			break
		}
	}
	if err := iter.Close(); err != nil {
		txErrors.Add(1)
		return 0, false
	}
	if !found {
		// tx not in our DB (very early blocks, or gap) → treat as real
		return 1, true
	}
	return s, true
}
