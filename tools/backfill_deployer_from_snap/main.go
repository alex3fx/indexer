// backfill_deployer_from_snap: reads creator+tx_hash+block_number from restored
// contracts_by_addresses snapshot and writes deployer+tx_hash to contracts_by_address_v2.
//
// Usage: backfill_deployer_from_snap --host 127.0.0.1 --pass cassandra [--workers N] [--dry-run]
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

const (
	defaultWorkers  = 64
	defaultPageSize = 5000
)

type job struct {
	address     string
	blockNumber int64
	creator     string
	txHash      string
}

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "Scylla user")
	pass     := flag.String("pass", "", "Scylla password")
	workers  := flag.Int("workers", defaultWorkers, "concurrent UPDATE workers")
	pageSize := flag.Int("page-size", defaultPageSize, "scan page size")
	dryRun   := flag.Bool("dry-run", false, "scan only, don't update")
	flag.Parse()

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

	var (
		scanned atomic.Int64
		skipped atomic.Int64 // creator is empty
		updated atomic.Int64
		dbErr   atomic.Int64
	)

	start := time.Now()

	jobs := make(chan job, *workers*4)
	var wg sync.WaitGroup

	// UPDATE workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const upd = `UPDATE eth.contracts_by_address_v2 SET deployer = ?, tx_hash = ? WHERE address = ? AND block_number = ?`
			for j := range jobs {
				if *dryRun {
					updated.Add(1)
					continue
				}
				err := retryQuery(func() error {
					return session.Query(upd, j.creator, j.txHash, j.address, j.blockNumber).Exec()
				})
				if err != nil {
					dbErr.Add(1)
					log.Printf("[err] update addr=%s blk=%d: %v", j.address, j.blockNumber, err)
				} else {
					updated.Add(1)
				}
			}
		}()
	}

	// Progress reporter
	go func() {
		for {
			time.Sleep(30 * time.Second)
			sc := scanned.Load()
			sk := skipped.Load()
			up := updated.Load()
			de := dbErr.Load()
			elapsed := time.Since(start)
			rate := float64(sc) / elapsed.Seconds()
			log.Printf("[progress] scanned=%d skipped=%d updated=%d dbErr=%d rate=%.0f rows/s elapsed=%s",
				sc, sk, up, de, rate, elapsed.Round(time.Second))
		}
	}()

	const sel = `SELECT address, block_number, creator, tx_hash FROM eth.contracts_by_addresses`

	log.Printf("Starting scan of eth.contracts_by_addresses (dry-run=%v, workers=%d)...", *dryRun, *workers)
	iter := session.Query(sel).PageSize(*pageSize).Iter()

	var (
		address     string
		blockNumber int64
		creator     string
		txHash      string
	)
	for iter.Scan(&address, &blockNumber, &creator, &txHash) {
		scanned.Add(1)
		if creator == "" {
			skipped.Add(1)
			continue
		}
		addrCopy := address
		jobs <- job{address: addrCopy, blockNumber: blockNumber, creator: creator, txHash: txHash}
	}
	if err := iter.Close(); err != nil {
		log.Printf("[scan] iter error: %v", err)
	}

	close(jobs)
	wg.Wait()

	sc := scanned.Load()
	sk := skipped.Load()
	up := updated.Load()
	de := dbErr.Load()
	elapsed := time.Since(start).Round(time.Second)

	fmt.Printf("=== DONE ===\n")
	fmt.Printf("scanned=%d  skipped=%d  updated=%d  dbErr=%d  elapsed=%s\n", sc, sk, up, de, elapsed)
	if de > 0 {
		fmt.Printf("WARNING: %d rows failed to update — re-run to retry\n", de)
	} else {
		fmt.Printf("OK — no errors\n")
	}
}

func retryQuery(fn func() error) error {
	const maxRetries = 8
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if attempt == maxRetries-1 {
			return fmt.Errorf("after %d retries: %w", maxRetries, err)
		}
		jitter := time.Duration(float64(delay) * (0.8 + 0.4*math.Sin(float64(attempt))))
		if jitter < 100*time.Millisecond {
			jitter = 100 * time.Millisecond
		}
		time.Sleep(jitter)
		delay = min(delay*2, 30*time.Second)
	}
	return nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
