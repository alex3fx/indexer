// count_null_deployer: scans contracts_by_address_v2 and counts rows where deployer is null/empty.
// Uses gocql paginated iterator — safe on 100M+ row tables.
package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	host    := flag.String("host", "127.0.0.1", "Scylla host")
	port    := flag.Int("port", 9042, "Scylla port")
	user    := flag.String("user", "cassandra", "Scylla user")
	pass    := flag.String("pass", "", "Scylla password")
	pageSize := flag.Int("page-size", 10000, "page size")
	flag.Parse()

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = "eth"
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 8
	cluster.Timeout = 120 * time.Second
	cluster.PageSize = *pageSize

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	var total, nullCount, nonNull atomic.Int64
	start := time.Now()

	go func() {
		for {
			time.Sleep(30 * time.Second)
			t := total.Load()
			n := nullCount.Load()
			elapsed := time.Since(start)
			rate := float64(t) / elapsed.Seconds()
			log.Printf("[progress] scanned=%d null=%d non_null=%d rate=%.0f rows/s elapsed=%s",
				t, n, nonNull.Load(), rate, elapsed.Round(time.Second))
		}
	}()

	log.Printf("Starting full scan of eth.contracts_by_address_v2...")
	iter := session.Query("SELECT deployer FROM eth.contracts_by_address_v2").Iter()
	var deployer string
	for iter.Scan(&deployer) {
		total.Add(1)
		if deployer == "" {
			nullCount.Add(1)
		} else {
			nonNull.Add(1)
		}
	}
	if err := iter.Close(); err != nil {
		log.Printf("iter error: %v", err)
	}

	t := total.Load()
	n := nullCount.Load()
	nn := nonNull.Load()
	elapsed := time.Since(start).Round(time.Second)
	fmt.Printf("=== DONE ===\n")
	fmt.Printf("total=%d  null=%d (%.2f%%)  non_null=%d  elapsed=%s\n",
		t, n, float64(n)/float64(t)*100, nn, elapsed)
}
