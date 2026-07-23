// reorg_scanner: находит реорги в диапазоне [from, to] сравнивая данные в Scylla
// с каноникальными данными из RPC.
//
// Два метода обнаружения:
//   FAST (block_hash != null в block_completions):  stored_hash vs canonical_hash.
//   SLOW (block_hash = null, v21-блоки без хранения хэша): сравнение tx hash sets.
//
// Изменений в БД не вносит — только пишет log-файл.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

const (
	chunkBuckets = int64(24)
	chunkEra     = int64(12000)
)

func blockToChunk(b int64) int32 {
	return int32((b%chunkBuckets) + chunkBuckets*(b/chunkEra))
}

// computeChunks returns all chunk IDs covering blocks in [from, to].
func computeChunks(from, to int64) []int32 {
	fromEra := from / chunkEra
	toEra := to / chunkEra
	var chunks []int32
	for era := fromEra; era <= toEra; era++ {
		for bucket := int64(0); bucket < chunkBuckets; bucket++ {
			chunk := int32(bucket + chunkBuckets*era)
			chunks = append(chunks, chunk)
		}
	}
	return chunks
}

// ── RPC ──────────────────────────────────────────────────────────────────────

type rpcReq struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type canonBlock struct {
	Hash         string   `json:"hash"`
	Transactions []string `json:"transactions"`
}

type rpcResp struct {
	Result *canonBlock `json:"result"`
}

func fetchCanonBlock(client *http.Client, rpcURL string, blockNumber int64) (*canonBlock, error) {
	hex := fmt.Sprintf("0x%x", blockNumber)
	payload, _ := json.Marshal(rpcReq{
		Jsonrpc: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{hex, false},
		ID:      1,
	})
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
		resp, err := client.Post(rpcURL, "application/json", bytes.NewReader(payload))
		if err != nil {
			lastErr = err
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var r rpcResp
		if err := json.Unmarshal(data, &r); err != nil {
			lastErr = fmt.Errorf("unmarshal: %w", err)
			continue
		}
		if r.Result == nil {
			lastErr = fmt.Errorf("null result for block %d", blockNumber)
			continue
		}
		r.Result.Hash = strings.ToLower(r.Result.Hash)
		for i := range r.Result.Transactions {
			r.Result.Transactions[i] = strings.ToLower(r.Result.Transactions[i])
		}
		return r.Result, nil
	}
	return nil, fmt.Errorf("rpc block %d: %w", blockNumber, lastErr)
}

// ── Scylla ───────────────────────────────────────────────────────────────────

type blockMeta struct {
	blockNumber int64
	blockHash   string // "" = NULL (v21), set for v22+
	txCount     int32
}

func loadChunkMetas(session *gocql.Session, chunk int32, from, to int64) ([]blockMeta, error) {
	iter := session.Query(
		`SELECT block_number, block_hash, tx_count FROM eth.block_completions
		 WHERE chunk=? AND block_number>=? AND block_number<=?`,
		chunk, from, to,
	).Iter()

	var metas []blockMeta
	var bn int64
	var bh *string
	var tc int32
	for iter.Scan(&bn, &bh, &tc) {
		var hash string
		if bh != nil {
			hash = strings.ToLower(*bh)
		}
		metas = append(metas, blockMeta{bn, hash, tc})
	}
	return metas, iter.Close()
}

func storedTxSet(session *gocql.Session, blockNumber int64) (map[string]struct{}, error) {
	chunk := blockToChunk(blockNumber)
	iter := session.Query(
		`SELECT hash FROM eth.transactions WHERE chunk=? AND block_number=?`,
		chunk, blockNumber,
	).Iter()
	set := make(map[string]struct{})
	var h string
	for iter.Scan(&h) {
		set[strings.ToLower(h)] = struct{}{}
	}
	return set, iter.Close()
}

// ── result ────────────────────────────────────────────────────────────────────

type reorgResult struct {
	blockNumber   int64
	method        string // "hash" | "tx"
	storedHash    string
	canonicalHash string
	storedTxCnt   int32
	canonTxCnt    int
	orphanOnly    int
	lost          int
}

// ── Redis cursor ──────────────────────────────────────────────────────────────

// readRedisCursor reads LATEST_PROCESSED_BLOCK_NUMBER via raw RESP protocol.
// redisAddr: "host:port", password and db are separate args.
func readRedisCursor(addr, password string, db int) (int64, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	w := bufio.NewWriter(conn)
	r := bufio.NewReader(conn)

	send := func(args ...string) error {
		fmt.Fprintf(w, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a)
		}
		return w.Flush()
	}
	readLine := func() (string, error) {
		line, err := r.ReadString('\n')
		return strings.TrimRight(line, "\r\n"), err
	}

	if password != "" {
		send("AUTH", password)
		if _, err := readLine(); err != nil {
			return 0, fmt.Errorf("AUTH: %w", err)
		}
	}
	send("SELECT", strconv.Itoa(db))
	if _, err := readLine(); err != nil {
		return 0, fmt.Errorf("SELECT: %w", err)
	}
	send("GET", "LATEST_PROCESSED_BLOCK_NUMBER")
	typeLine, err := readLine()
	if err != nil {
		return 0, err
	}
	if typeLine == "$-1" {
		return 0, fmt.Errorf("key not found")
	}
	// typeLine = $N (length)
	valLine, err := readLine()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(valLine, 10, 64)
}

// parseRedisURL parses redis://:password@host:port/db
func parseRedisURL(u string) (addr, password string, db int, err error) {
	u = strings.TrimPrefix(u, "redis://")
	// split at @
	atIdx := strings.LastIndex(u, "@")
	var hostPart, authPart string
	if atIdx >= 0 {
		authPart = u[:atIdx]
		hostPart = u[atIdx+1:]
	} else {
		hostPart = u
	}
	// auth: :password or user:password
	if authPart != "" {
		parts := strings.SplitN(authPart, ":", 2)
		if len(parts) == 2 {
			password = parts[1]
		} else {
			password = parts[0]
		}
	}
	// hostPart: host:port/db
	slashIdx := strings.Index(hostPart, "/")
	if slashIdx >= 0 {
		db, _ = strconv.Atoi(hostPart[slashIdx+1:])
		hostPart = hostPart[:slashIdx]
	}
	addr = hostPart
	if !strings.Contains(addr, ":") {
		addr += ":6379"
	}
	return
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	fromFlag := flag.Int64("from", 25422404, "start block (inclusive)")
	toFlag := flag.Int64("to", 0, "end block (inclusive); 0 = read from Redis cursor")
	rpcURL := flag.String("rpc", "http://100.64.0.60:8545", "Ethereum RPC URL")
	host := flag.String("host", "127.0.0.1", "Scylla host")
	port := flag.Int("port", 9042, "Scylla port")
	user := flag.String("user", "cassandra", "Scylla user")
	pass := flag.String("pass", "cassandra", "Scylla password")
	workers := flag.Int("workers", 16, "parallel workers")
	output := flag.String("output", "", "log file path (default: reorg_scan_FROM_TO_TIMESTAMP.log)")
	redisURL := flag.String("redis", "redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/2", "Redis URL for cursor")
	flag.Parse()

	fromBlock := *fromFlag
	toBlock := *toFlag

	if toBlock == 0 {
		addr, password, db, err := parseRedisURL(*redisURL)
		if err != nil {
			log.Printf("warn: cannot parse redis URL (%v); use --to to set end block", err)
		} else {
			n, err := readRedisCursor(addr, password, db)
			if err != nil {
				log.Printf("warn: cannot read Redis cursor (%v); use --to to set end block", err)
			} else {
				toBlock = n
				log.Printf("to-block from Redis: %d", toBlock)
			}
		}
		if toBlock == 0 {
			log.Fatal("--to is required when Redis cursor is unavailable")
		}
	}

	logPath := *output
	if logPath == "" {
		logPath = fmt.Sprintf("reorg_scan_%d_%d_%s.log", fromBlock, toBlock, time.Now().Format("20060102_150405"))
	}
	logF, err := os.Create(logPath)
	if err != nil {
		log.Fatalf("create log: %v", err)
	}
	defer logF.Close()

	logf := func(format string, args ...interface{}) {
		s := fmt.Sprintf(format, args...)
		fmt.Print(s)
		fmt.Fprint(logF, s)
	}

	logf("=== reorg_scanner from=%d to=%d workers=%d ===\n", fromBlock, toBlock, *workers)
	logf("started: %s\n\n", time.Now().Format(time.RFC3339))

	// Scylla session
	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: *user, Password: *pass}
	cluster.Keyspace = "eth"
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 30 * time.Second
	cluster.NumConns = *workers + 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("scylla connect: %v", err)
	}
	defer session.Close()

	client := &http.Client{Timeout: 30 * time.Second}

	// load all block metas from Scylla (producer goroutine)
	chunks := computeChunks(fromBlock, toBlock)
	logf("chunks: %d\n\n", len(chunks))

	type workItem struct{ meta blockMeta }
	workCh := make(chan workItem, 50000)

	go func() {
		for _, chunk := range chunks {
			metas, err := loadChunkMetas(session, chunk, fromBlock, toBlock)
			if err != nil {
				logf("ERROR load chunk %d: %v\n", chunk, err)
				continue
			}
			for _, m := range metas {
				workCh <- workItem{m}
			}
		}
		close(workCh)
	}()

	type rawResult struct {
		blockNumber int64
		isReorg     bool
		r           *reorgResult
		errMsg      string
	}
	resultCh := make(chan rawResult, 5000)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workCh {
				m := item.meta
				rr, errMsg := processBlock(client, session, *rpcURL, m)
				resultCh <- rawResult{m.blockNumber, rr != nil, rr, errMsg}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// collect results
	var totalBlocks, totalReorgs, totalErrors atomic.Int64
	var mu sync.Mutex
	var reorgList []reorgResult
	var errList []string

	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for rr := range resultCh {
			t := totalBlocks.Add(1)
			if t%50000 == 0 {
				fmt.Printf("\rprogress: %d blocks | reorgs: %d | errors: %d   ",
					t, totalReorgs.Load(), totalErrors.Load())
			}
			if rr.errMsg != "" {
				totalErrors.Add(1)
				mu.Lock()
				errList = append(errList, fmt.Sprintf("blk=%d %s", rr.blockNumber, rr.errMsg))
				mu.Unlock()
				continue
			}
			if rr.isReorg {
				totalReorgs.Add(1)
				mu.Lock()
				reorgList = append(reorgList, *rr.r)
				mu.Unlock()
			}
		}
	}()
	collectWg.Wait()
	fmt.Println()

	// sort and output
	sort.Slice(reorgList, func(i, j int) bool { return reorgList[i].blockNumber < reorgList[j].blockNumber })

	logf("\n=== REORGS DETECTED (%d) ===\n", len(reorgList))
	for _, r := range reorgList {
		switch r.method {
		case "hash":
			logf("REORG  blk=%-10d  method=hash  stored_hash=%s  canonical_hash=%s\n",
				r.blockNumber, r.storedHash, r.canonicalHash)
		case "tx":
			orphanTag := ""
			if r.orphanOnly > 0 {
				orphanTag = fmt.Sprintf("orphan_only=%d  ", r.orphanOnly)
			}
			lostTag := ""
			if r.lost > 0 {
				lostTag = fmt.Sprintf("lost=%d  ", r.lost)
			}
			logf("REORG  blk=%-10d  method=tx  bc_tx=%d  canon_tx=%d  %s%scanonical_hash=%s\n",
				r.blockNumber, r.storedTxCnt, r.canonTxCnt, orphanTag, lostTag, r.canonicalHash)
		}
	}

	if len(errList) > 0 {
		logf("\n=== ERRORS (%d) ===\n", len(errList))
		for _, e := range errList {
			logf("  %s\n", e)
		}
	}

	nHash := countMethod(reorgList, "hash")
	nTx := countMethod(reorgList, "tx")

	logf("\n=== SUMMARY ===\n")
	logf("range:         %d – %d\n", fromBlock, toBlock)
	logf("blocks_scanned: %d\n", totalBlocks.Load())
	logf("reorgs_found:  %d\n", totalReorgs.Load())
	logf("  method=hash: %d  (v22 blocks: block_hash stored → hash mismatch)\n", nHash)
	logf("  method=tx:   %d  (v21 blocks: no block_hash → tx set mismatch)\n", nTx)
	logf("errors:        %d\n", totalErrors.Load())
	logf("log:           %s\n", logPath)
	logf("finished:      %s\n", time.Now().Format(time.RFC3339))
}

func processBlock(client *http.Client, session *gocql.Session, rpcURL string, m blockMeta) (*reorgResult, string) {
	cb, err := fetchCanonBlock(client, rpcURL, m.blockNumber)
	if err != nil {
		return nil, err.Error()
	}

	if m.blockHash != "" {
		// FAST PATH: compare block hashes
		if strings.EqualFold(m.blockHash, cb.Hash) {
			return nil, "" // OK
		}
		return &reorgResult{
			blockNumber:   m.blockNumber,
			method:        "hash",
			storedHash:    m.blockHash,
			canonicalHash: cb.Hash,
			storedTxCnt:   m.txCount,
			canonTxCnt:    len(cb.Transactions),
		}, ""
	}

	// SLOW PATH: compare tx hash sets
	canonSet := make(map[string]struct{}, len(cb.Transactions))
	for _, h := range cb.Transactions {
		canonSet[h] = struct{}{}
	}

	stored, err := storedTxSet(session, m.blockNumber)
	if err != nil {
		return nil, fmt.Sprintf("storedTxSet: %v", err)
	}

	orphanOnly, lost := 0, 0
	for h := range stored {
		if _, ok := canonSet[h]; !ok {
			orphanOnly++
		}
	}
	for h := range canonSet {
		if _, ok := stored[h]; !ok {
			lost++
		}
	}

	if orphanOnly == 0 && lost == 0 {
		return nil, "" // OK
	}
	return &reorgResult{
		blockNumber:   m.blockNumber,
		method:        "tx",
		canonicalHash: cb.Hash,
		storedTxCnt:   m.txCount,
		canonTxCnt:    len(cb.Transactions),
		orphanOnly:    orphanOnly,
		lost:          lost,
	}, ""
}

func countMethod(list []reorgResult, method string) int {
	n := 0
	for _, r := range list {
		if r.method == method {
			n++
		}
	}
	return n
}
