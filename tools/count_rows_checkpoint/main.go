// count_rows_checkpoint: counts rows in chunk-partitioned tables up to a fixed block number.
// Used for cross-validation against Dune Analytics.
//
// Tables counted: blocks, transactions, logs, internal_transactions
// Chunk formula: chunk = (block % LANES) + LANES * (block // ERA_SIZE)
//
// For each table runs: SELECT COUNT(*) WHERE chunk=? AND block_number <= checkpoint
// (blocks table uses column "number" instead of "block_number")
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

const (
	defaultLanes   = 24
	defaultEraSize = 12000
)

func main() {
	host       := flag.String("host", "127.0.0.1", "Scylla host")
	port       := flag.Int("port", 9042, "Scylla port")
	user       := flag.String("user", "cassandra", "user")
	pass       := flag.String("pass", "", "password")
	keyspace   := flag.String("keyspace", "eth", "keyspace")
	from       := flag.Int64("from", 0, "start block number (inclusive, 0 = genesis)")
	checkpoint := flag.Int64("checkpoint", 0, "end block number (inclusive upper bound)")
	workers    := flag.Int("workers", 8, "parallel workers per table")
	timeout    := flag.Duration("timeout", 120*time.Second, "Scylla query timeout")
	retries    := flag.Int("retries", 8, "retry attempts per chunk")
	table      := flag.String("table", "", "count only this table (empty = all)")
	lanes      := flag.Int("lanes", defaultLanes, "chunk lanes")
	eraSize    := flag.Int64("era-size", defaultEraSize, "blocks per era")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
	}
	if *checkpoint == 0 {
		log.Fatal("--checkpoint required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = *timeout
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// Build chunk list: (lane, era) pairs covering blocks from..checkpoint
	minEra := *from / *eraSize
	maxEra := *checkpoint / *eraSize
	var chunks []int
	for era := minEra; era <= maxEra; era++ {
		for lane := 0; lane < *lanes; lane++ {
			chunks = append(chunks, lane+*lanes*int(era))
		}
	}
	log.Printf("from=%d  checkpoint=%d  eras=%d-%d  chunks=%d  workers=%d  timeout=%s  retries=%d",
		*from, *checkpoint, minEra, maxEra, len(chunks), *workers, *timeout, *retries)

	type tableSpec struct {
		name  string
		bnCol string
	}
	allTables := []tableSpec{
		{"blocks", "number"},
		{"transactions", "block_number"},
		{"logs", "block_number"},
		{"internal_transactions", "block_number"},
	}

	var tables []tableSpec
	if *table != "" {
		for _, t := range allTables {
			if strings.EqualFold(t.name, *table) {
				tables = append(tables, t)
			}
		}
		if len(tables) == 0 {
			log.Fatalf("unknown table %q; valid: blocks, transactions, logs, internal_transactions", *table)
		}
	} else {
		tables = allTables
	}

	fmt.Printf("\n%-30s  %15s  %12s  %6s\n", "table", "rows", "elapsed", "errors")
	fmt.Println("  " + "─────────────────────────────────────────────────────────")

	start := time.Now()
	for _, tbl := range tables {
		count, errs, elapsed := countTable(session, tbl.name, tbl.bnCol, chunks, *from, *checkpoint, *workers, *retries)
		fmt.Printf("%-30s  %15d  %12.1fs  %6d\n", tbl.name, count, elapsed.Seconds(), errs)
	}

	fmt.Printf("\nTotal elapsed: %.1fs\n", time.Since(start).Seconds())
	fmt.Printf("Checkpoint block: %d\n", *checkpoint)
}

func countTable(session *gocql.Session, table, bnCol string, chunks []int, from, checkpoint int64, numWorkers, maxRetries int) (int64, int64, time.Duration) {
	start := time.Now()

	chunkCh := make(chan int, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	var total atomic.Int64
	var errors atomic.Int64
	var done atomic.Int64

	logTicker := time.NewTicker(30 * time.Second)
	stopLog := make(chan struct{})
	go func() {
		for {
			select {
			case <-logTicker.C:
				log.Printf("[%s] chunks=%d/%d rows=%d errors=%d",
					table, done.Load(), len(chunks), total.Load(), errors.Load())
			case <-stopLog:
				logTicker.Stop()
				return
			}
		}
	}()

	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE chunk=? AND %s>=? AND %s<=?`, table, bnCol, bnCol)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkCh {
				var count int64
				err := retryCount(session, query, chunk, from, checkpoint, &count, maxRetries)
				if err != nil {
					errors.Add(1)
					log.Printf("[ERROR] %s chunk=%d: %v", table, chunk, err)
				} else {
					total.Add(count)
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	close(stopLog)

	return total.Load(), errors.Load(), time.Since(start)
}

func retryCount(session *gocql.Session, query string, chunk int, from, checkpoint int64, count *int64, maxRetries int) error {
	var delay time.Duration = 500 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			if delay < 64*time.Second {
				delay *= 2
			}
		}
		err := session.Query(query, chunk, from, checkpoint).Scan(count)
		if err == nil {
			return nil
		}
		log.Printf("[retry %d/%d] chunk=%d: %v", attempt+1, maxRetries, chunk, err)
	}
	return fmt.Errorf("all %d retries failed (chunk=%d)", maxRetries, chunk)
}
