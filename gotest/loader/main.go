// loader — loads a zigtest2 binary dump directly into ScyllaDB.
// Two modes:
//   -mode=batch  (default) saveBatch per dump-batch: mirrors zigtest2 parser exactly.
//                For each 10-block batch: split rows by table, spawn parallel workers,
//                each worker sends UNLOGGED BATCH frames, wait, then next batch.
//   -mode=firehose  legacy: feed all rows into per-table channels, workers drain continuously.
//
// Usage:
//
//	./loader -dump=dump_1000.bin -truncate
//	./loader -dump=dump_1000.bin -mode=firehose
//	./loader -dump=dump_1000.bin -analyze
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAGIC        = "ZIG2D\x01"
	BATCH_MARKER = 0xFF
	TABLE_BLOCKS = 0
	TABLE_TXS    = 1
	TABLE_LOGS   = 2
	TABLE_ITXS   = 3
	TABLE_CONTS  = 4
	TABLE_CBA    = 5
	TABLE_COUNT  = 6
	CQL_VER      = 0x04
	OP_STARTUP   = 0x01
	OP_AUTH_RESP = 0x0F
	OP_QUERY     = 0x07
	OP_PREPARE   = 0x09
	OP_EXECUTE   = 0x0A
	OP_BATCH     = 0x0D
	OP_READY     = 0x02
	OP_AUTH      = 0x03
	OP_AUTH_OK   = 0x10
	OP_RESULT    = 0x08
	OP_ERROR     = 0x00
)

var (
	flagDump       = flag.String("dump", "dump_1000.bin", "Dump file path")
	flagHost       = flag.String("host", "172.31.208.104:9142", "ScyllaDB host:port")
	flagKS         = flag.String("ks", "eth", "Keyspace")
	flagUser       = flag.String("user", "cassandra", "Username")
	flagPass       = flag.String("pass", "cassandra", "Password")
	flagSplit      = flag.String("split", "1,3,6,20,1,1", "Connections per table (blocks,txs,logs,itxs,contracts,cba)")
	flagBatchSplit = flag.String("batch-split", "100,100,100,100,100,100", "Rows per UNLOGGED BATCH frame per table")
	flagMode       = flag.String("mode", "batch", "Mode: batch (saveBatch per dump-batch) or firehose")
	flagRemapMod      = flag.Int("remap-mod", 0, "Remap chunk = block_number %% N for tables 0-4 (0=off)")
	flagBlocksPerBatch = flag.Int("blocks-per-batch", 0, "Group this many blocks per saveBatch call (0=use dump batches as-is)")
	flagTrunc      = flag.Bool("truncate", false, "TRUNCATE target tables before benchmark")
	flagAnalyze    = flag.Bool("analyze", false, "Only read and summarize dump metadata")
)

var insertStmts = [TABLE_COUNT]string{
	"INSERT INTO blocks (chunk,number,timestamp_s,timestamp_ms,miner) VALUES (?,?,?,?,?)",
	"INSERT INTO transactions (chunk,block_number,transaction_index,hash,block_timestamp_s,block_timestamp_ms,method_id,input,from_address,to_address,value,gas_limit,gas_price,gas_used,max_priority_fee_per_gas,max_fee_per_gas,cumulative_gas_used,effective_gas_price,contract_address,status,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
	"INSERT INTO logs (chunk,block_number,transaction_index,log_index,block_timestamp_s,block_timestamp_ms,address,data,topic_zeroth,topic_first,topic_second,topic_third,rest_topics,transaction_hash,removed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
	"INSERT INTO internal_transactions (chunk,block_number,block_timestamp_s,block_timestamp_ms,transaction_index,transaction_hash,trace_index,from_address,to_address,value) VALUES (?,?,?,?,?,?,?,?,?,?)",
	"INSERT INTO contracts (chunk,block_number,transaction_index,transaction_hash,trace_index,block_timestamp_s,block_timestamp_ms,address,creation_method,creator_address,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
	"INSERT INTO contracts_by_addresses (address,creator,tx_hash,block_number,timestamp,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?)",
}

var nValues = [TABLE_COUNT]uint16{5, 21, 15, 10, 13, 8}

func parseSplit(s string) ([TABLE_COUNT]int, error) {
	parts := strings.Split(s, ",")
	if len(parts) != TABLE_COUNT {
		return [TABLE_COUNT]int{}, fmt.Errorf("need %d values, got %d", TABLE_COUNT, len(parts))
	}
	var r [TABLE_COUNT]int
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v < 1 {
			return [TABLE_COUNT]int{}, fmt.Errorf("split[%d] invalid: %q", i, p)
		}
		r[i] = v
	}
	return r, nil
}

// ─── CQL raw connection ───────────────────────────────────────────────────────

type CQLConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func newCQLConn(addr, ks, user, pass string) (*CQLConn, error) {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c.(*net.TCPConn).SetNoDelay(true)
	cc := &CQLConn{conn: c, r: bufio.NewReaderSize(c, 65536)}
	if err := cc.handshake(ks, user, pass); err != nil {
		c.Close()
		return nil, err
	}
	return cc, nil
}

func (c *CQLConn) close() { c.conn.Close() }

func (c *CQLConn) sendFrame(op byte, body []byte, stream uint16) error {
	hdr := [9]byte{CQL_VER, 0, byte(stream >> 8), byte(stream), op}
	binary.BigEndian.PutUint32(hdr[5:], uint32(len(body)))
	if _, err := c.conn.Write(hdr[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		_, err := c.conn.Write(body)
		return err
	}
	return nil
}

func (c *CQLConn) recvFrame() (op byte, body []byte, err error) {
	hdr := make([]byte, 9)
	if _, err = io.ReadFull(c.r, hdr); err != nil {
		return
	}
	op = hdr[4]
	n := binary.BigEndian.Uint32(hdr[5:])
	if n == 0 {
		return
	}
	body = make([]byte, n)
	_, err = io.ReadFull(c.r, body)
	return
}

func cqlErr(body []byte) error {
	if len(body) < 6 {
		return fmt.Errorf("cql error: short body")
	}
	code := binary.BigEndian.Uint32(body[0:4])
	msgLen := int(binary.BigEndian.Uint16(body[4:6]))
	msg := ""
	if 6+msgLen <= len(body) {
		msg = string(body[6 : 6+msgLen])
	}
	return fmt.Errorf("cql 0x%08x: %s", code, msg)
}

func (c *CQLConn) recvResult() error {
	op, body, err := c.recvFrame()
	if err != nil {
		return err
	}
	if op == OP_ERROR {
		return cqlErr(body)
	}
	if op != OP_RESULT && op != OP_READY && op != OP_AUTH_OK {
		return fmt.Errorf("unexpected op=%02x", op)
	}
	return nil
}

func cqlStr(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

func cqlLongStr(s string) []byte {
	b := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(b, uint32(len(s)))
	copy(b[4:], s)
	return b
}

func (c *CQLConn) handshake(ks, user, pass string) error {
	body := make([]byte, 0, 64)
	body = append(body, 0, 1)
	body = append(body, cqlStr("CQL_VERSION")...)
	body = append(body, cqlStr("3.0.0")...)
	if err := c.sendFrame(OP_STARTUP, body, 1); err != nil {
		return err
	}
	op, b, err := c.recvFrame()
	if err != nil {
		return err
	}
	if op == OP_AUTH {
		sasl := append([]byte{0}, append([]byte(user), append([]byte{0}, []byte(pass)...)...)...)
		ab := make([]byte, 4+len(sasl))
		binary.BigEndian.PutUint32(ab, uint32(len(sasl)))
		copy(ab[4:], sasl)
		if err := c.sendFrame(OP_AUTH_RESP, ab, 1); err != nil {
			return err
		}
		op, b, err = c.recvFrame()
		if err != nil {
			return err
		}
		if op != OP_AUTH_OK {
			return fmt.Errorf("auth failed op=%02x", op)
		}
	} else if op != OP_READY {
		return fmt.Errorf("unexpected startup op=%02x", op)
	}
	_ = b
	qbody := append(cqlLongStr("USE "+ks), 0x00, 0x01, 0x00)
	if err := c.sendFrame(OP_QUERY, qbody, 1); err != nil {
		return err
	}
	return c.recvResult()
}

func (c *CQLConn) prepare(stmt string) ([]byte, error) {
	body := append(cqlLongStr(stmt), 0, 0, 0, 0)
	if err := c.sendFrame(OP_PREPARE, body, 1); err != nil {
		return nil, err
	}
	op, b, err := c.recvFrame()
	if err != nil {
		return nil, err
	}
	if op != OP_RESULT {
		return nil, fmt.Errorf("prepare failed op=%02x", op)
	}
	if len(b) < 6 {
		return nil, fmt.Errorf("prepare result too short")
	}
	idLen := binary.BigEndian.Uint16(b[4:6])
	return b[6 : 6+idLen], nil
}

// sendBatch sends one UNLOGGED BATCH and reads the response.
func (c *CQLConn) sendBatch(prepID []byte, nvals uint16, rowVals [][]byte) error {
	if len(rowVals) == 0 {
		return nil
	}
	body := make([]byte, 0, 6+len(rowVals)*(3+2+len(prepID)+2+300))
	body = append(body, 0x01) // UNLOGGED
	body = append(body, byte(len(rowVals)>>8), byte(len(rowVals)))
	for _, rv := range rowVals {
		body = append(body, 0x01)
		body = append(body, byte(len(prepID)>>8), byte(len(prepID)))
		body = append(body, prepID...)
		body = append(body, byte(nvals>>8), byte(nvals))
		body = append(body, rv...)
	}
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, 0x00)       // flags=0
	if err := c.sendFrame(OP_BATCH, body, 1); err != nil {
		return err
	}
	op, b, err := c.recvFrame()
	if err != nil {
		return err
	}
	if op == OP_ERROR {
		return cqlErr(b)
	}
	return nil
}

func (c *CQLConn) query(stmt string) error {
	qbody := append(cqlLongStr(stmt), 0x00, 0x01, 0x00)
	if err := c.sendFrame(OP_QUERY, qbody, 1); err != nil {
		return err
	}
	return c.recvResult()
}

// ─── Row / Batch types ────────────────────────────────────────────────────────

type Row struct {
	TableID uint8
	NValues uint16
	Values  []byte
}

type Batch struct {
	ID   uint32
	Rows []Row
}

// ─── Pool: persistent per-table connections ───────────────────────────────────

type Pool struct {
	conns   [TABLE_COUNT][]*CQLConn
	prepIDs [TABLE_COUNT][]byte
}

func newPool(split [TABLE_COUNT]int, addr, ks, user, pass string) (*Pool, error) {
	p := &Pool{}
	for t := 0; t < TABLE_COUNT; t++ {
		p.conns[t] = make([]*CQLConn, split[t])
		for w := 0; w < split[t]; w++ {
			c, err := newCQLConn(addr, ks, user, pass)
			if err != nil {
				return nil, fmt.Errorf("conn table=%d w=%d: %w", t, w, err)
			}
			p.conns[t][w] = c
			if w == 0 {
				pid, err := c.prepare(insertStmts[t])
				if err != nil {
					return nil, fmt.Errorf("prepare table=%d: %w", t, err)
				}
				p.prepIDs[t] = pid
			} else {
				// Every connection must prepare its own statement (shard-local)
				pid, err := c.prepare(insertStmts[t])
				if err != nil {
					return nil, fmt.Errorf("prepare table=%d w=%d: %w", t, w, err)
				}
				// We reuse the same prepID bytes (server assigns same ID on same shard)
				_ = pid
				p.prepIDs[t] = p.prepIDs[t] // keep first conn's prepID
			}
		}
	}
	return p, nil
}

func (p *Pool) close() {
	for t := 0; t < TABLE_COUNT; t++ {
		for _, c := range p.conns[t] {
			c.close()
		}
	}
}

// ─── saveBatch: mirrors zigtest2 db.zig saveBatch ─────────────────────────────
// Separates rows by table, spawns parallel workers per table, each sends BATCH frames.
// Waits for all workers before returning — matches parser's per-batch semantics.

func (p *Pool) saveBatch(batch *Batch, batchSizes [TABLE_COUNT]int, errCount *atomic.Int64, firstErr *atomic.Value) int64 {
	// Separate rows by table
	var tableRows [TABLE_COUNT][][]byte
	for t := range tableRows {
		tableRows[t] = make([][]byte, 0, 64)
	}
	for _, row := range batch.Rows {
		if row.TableID < TABLE_COUNT {
			tableRows[row.TableID] = append(tableRows[row.TableID], row.Values)
		}
	}

	var wg sync.WaitGroup
	var written atomic.Int64

	for t := 0; t < TABLE_COUNT; t++ {
		rows := tableRows[t]
		if len(rows) == 0 {
			continue
		}
		conns := p.conns[t]
		prepID := p.prepIDs[t]
		nv := nValues[t]
		bs := batchSizes[t]
		n := len(conns)

		// Distribute rows evenly across connections (like spawnTable in Zig)
		perConn := (len(rows) + n - 1) / n
		for w := 0; w < n; w++ {
			start := w * perConn
			if start >= len(rows) {
				break
			}
			end := start + perConn
			if end > len(rows) {
				end = len(rows)
			}
			slice := rows[start:end]
			conn := conns[w]

			wg.Add(1)
			go func() {
				defer wg.Done()
				// Send in BATCH frames of bs rows
				for i := 0; i < len(slice); i += bs {
					e := i + bs
					if e > len(slice) {
						e = len(slice)
					}
					if err := conn.sendBatch(prepID, nv, slice[i:e]); err != nil {
						errCount.Add(1)
						if firstErr.Load() == nil {
							firstErr.Store(err.Error())
						}
						return
					}
					written.Add(int64(e - i))
				}
			}()
		}
	}
	wg.Wait()
	return written.Load()
}

// ─── Firehose mode (legacy) ───────────────────────────────────────────────────

type FHWorker struct {
	conn      *CQLConn
	prepID    []byte
	nvals     uint16
	batchSize int
	rowCh     chan Row
	wg        *sync.WaitGroup
	errCount  *atomic.Int64
	written   *atomic.Int64
	firstErr  *atomic.Value
}

func (w *FHWorker) run() {
	defer w.wg.Done()
	buf := make([][]byte, 0, w.batchSize)
	for row := range w.rowCh {
		buf = append(buf, row.Values)
		if len(buf) >= w.batchSize {
			w.flush(buf)
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		w.flush(buf)
	}
}

func (w *FHWorker) flush(rv [][]byte) {
	if err := w.conn.sendBatch(w.prepID, w.nvals, rv); err != nil {
		w.errCount.Add(1)
		if w.firstErr.Load() == nil {
			w.firstErr.Store(err.Error())
		}
		return
	}
	w.written.Add(int64(len(rv)))
}

func runFirehose(batches []Batch, split [TABLE_COUNT]int, batchSizes [TABLE_COUNT]int, addr, ks, user, pass string) (time.Duration, int64, string, error) {
	var allConns []*CQLConn
	defer func() {
		for _, c := range allConns {
			c.close()
		}
	}()
	var wg sync.WaitGroup
	var errCount, rowCount atomic.Int64
	var firstErr atomic.Value
	var chans [TABLE_COUNT]chan Row

	for t := 0; t < TABLE_COUNT; t++ {
		n := split[t]
		chans[t] = make(chan Row, n*batchSizes[t]*4)
		for w := 0; w < n; w++ {
			c, err := newCQLConn(addr, ks, user, pass)
			if err != nil {
				return 0, 0, "", fmt.Errorf("connect t=%d w=%d: %w", t, w, err)
			}
			allConns = append(allConns, c)
			pid, err := c.prepare(insertStmts[t])
			if err != nil {
				return 0, 0, "", fmt.Errorf("prepare t=%d w=%d: %w", t, w, err)
			}
			fw := &FHWorker{conn: c, prepID: pid, nvals: nValues[t], batchSize: batchSizes[t],
				rowCh: chans[t], wg: &wg, errCount: &errCount, written: &rowCount, firstErr: &firstErr}
			wg.Add(1)
			go fw.run()
		}
	}
	t0 := time.Now()
	for _, batch := range batches {
		for _, row := range batch.Rows {
			if row.TableID < TABLE_COUNT {
				chans[row.TableID] <- row
			}
		}
	}
	for t := 0; t < TABLE_COUNT; t++ {
		close(chans[t])
	}
	wg.Wait()
	elapsed := time.Since(t0)
	if errCount.Load() > 0 {
		fmt.Fprintf(os.Stderr, "  CQL errors: %d\n", errCount.Load())
	}
	errMsg := ""
	if v := firstErr.Load(); v != nil {
		errMsg = v.(string)
	}
	return elapsed, rowCount.Load(), errMsg, nil
}

// ─── Dump loader ──────────────────────────────────────────────────────────────

func loadDump(path string) ([]Batch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	magic := make([]byte, 6)
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != MAGIC {
		return nil, fmt.Errorf("bad magic: %x", magic)
	}
	var batches []Batch
	var cur *Batch
	for {
		tb, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if tb == BATCH_MARKER {
			hdr := make([]byte, 9)
			if _, err := io.ReadFull(r, hdr); err != nil {
				return nil, err
			}
			if cur != nil {
				batches = append(batches, *cur)
			}
			cur = &Batch{
				ID:   binary.BigEndian.Uint32(hdr[1:5]),
				Rows: make([]Row, 0, binary.BigEndian.Uint32(hdr[5:9])),
			}
		} else {
			nvBuf := make([]byte, 6)
			if _, err := io.ReadFull(r, nvBuf); err != nil {
				return nil, err
			}
			nvals := binary.BigEndian.Uint16(nvBuf[0:2])
			vlen := binary.BigEndian.Uint32(nvBuf[2:6])
			vals := make([]byte, vlen)
			if _, err := io.ReadFull(r, vals); err != nil {
				return nil, err
			}
			if cur != nil {
				cur.Rows = append(cur.Rows, Row{TableID: tb, NValues: nvals, Values: vals})
			}
		}
	}
	if cur != nil {
		batches = append(batches, *cur)
	}
	return batches, nil
}

// mergeDumpBatches combines every n consecutive dump batches into one.
// Each dump batch contains exactly blocksInDumpBatch blocks (e.g. 8).
// With n=1: batch as-is; n=2: 2× blocks per saveBatch call; n=4: 4× blocks.
func mergeDumpBatches(batches []Batch, n int) []Batch {
	if n <= 1 {
		return batches
	}
	result := make([]Batch, 0, len(batches)/n+1)
	for i := 0; i < len(batches); i += n {
		end := i + n
		if end > len(batches) {
			end = len(batches)
		}
		merged := Batch{ID: uint32(len(result))}
		totalRows := 0
		for _, b := range batches[i:end] {
			totalRows += len(b.Rows)
		}
		merged.Rows = make([]Row, 0, totalRows)
		for _, b := range batches[i:end] {
			merged.Rows = append(merged.Rows, b.Rows...)
		}
		result = append(result, merged)
	}
	return result
}

// remapChunks rewrites chunk = block_number % mod in-place for tables 0-4.
// Values encoding for these tables: [int32 chunk][bigint block_number]...
//   chunk:        [4B len=4][4B value]  → bytes [0:8]
//   block_number: [4B len=8][8B value]  → bytes [8:20]
// contracts_by_addresses (table 5) starts with address — no chunk field, skip.
func remapChunks(batches []Batch, mod int64) {
	for i := range batches {
		for j := range batches[i].Rows {
			row := &batches[i].Rows[j]
			if row.TableID >= TABLE_CBA || len(row.Values) < 20 {
				continue
			}
			blockNum := int64(binary.BigEndian.Uint64(row.Values[12:20]))
			newChunk := int32(blockNum % mod)
			binary.BigEndian.PutUint32(row.Values[4:8], uint32(newChunk))
		}
	}
}

func printDumpStats(batches []Batch) {
	var rowsByTable [TABLE_COUNT]int64
	chunks := make(map[int32]struct{})
	for _, batch := range batches {
		for _, row := range batch.Rows {
			if row.TableID < TABLE_COUNT {
				rowsByTable[row.TableID]++
				if row.TableID != TABLE_CBA && len(row.Values) >= 8 {
					if int(binary.BigEndian.Uint32(row.Values[0:4])) == 4 {
						chunk := int32(binary.BigEndian.Uint32(row.Values[4:8]))
						chunks[chunk] = struct{}{}
					}
				}
			}
		}
	}
	names := [TABLE_COUNT]string{"blocks", "transactions", "logs", "internal_transactions", "contracts", "contracts_by_addresses"}
	fmt.Printf("Dump summary:\n")
	for i, n := range names {
		fmt.Printf("  %-24s %d rows\n", n, rowsByTable[i])
	}
	cs := make([]int, 0, len(chunks))
	for c := range chunks {
		cs = append(cs, int(c))
	}
	sort.Ints(cs)
	fmt.Printf("  unique chunk keys: %d", len(cs))
	if len(cs) > 0 && len(cs) <= 32 {
		fmt.Printf(" %v", cs)
	} else if len(cs) > 32 {
		fmt.Printf(" [%d..%d]", cs[0], cs[len(cs)-1])
	}
	fmt.Println()
	fmt.Println()
}

func truncateTables(addr, ks, user, pass string) error {
	c, err := newCQLConn(addr, ks, user, pass)
	if err != nil {
		return err
	}
	defer c.close()
	for _, t := range []string{"transactions", "logs", "internal_transactions", "blocks", "contracts", "contracts_by_addresses"} {
		if err := c.query("TRUNCATE " + t); err != nil {
			return fmt.Errorf("truncate %s: %w", t, err)
		}
	}
	return nil
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	split, err := parseSplit(*flagSplit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "split error: %v\n", err)
		os.Exit(1)
	}
	batchSizes, err := parseSplit(*flagBatchSplit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "batch-split error: %v\n", err)
		os.Exit(1)
	}
	total := 0
	for _, n := range split {
		total += n
	}

	fmt.Printf("Loading dump: %s\n", *flagDump)
	t0 := time.Now()
	batches, err := loadDump(*flagDump)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load error: %v\n", err)
		os.Exit(1)
	}
	totalRows := int64(0)
	for _, b := range batches {
		totalRows += int64(len(b.Rows))
	}
	fmt.Printf("  %d batches, %d rows in %v\n\n", len(batches), totalRows, time.Since(t0).Round(time.Millisecond))

	if *flagRemapMod > 0 {
		remapChunks(batches, int64(*flagRemapMod))
		fmt.Printf("  chunk remapped: block_number %% %d\n", *flagRemapMod)
	}
	if *flagBlocksPerBatch > 0 {
		// Each dump batch has exactly blocksInDump blocks.
		// Count blocks in first dump batch to find blocksInDump.
		blocksInDump := 0
		if len(batches) > 0 {
			for _, row := range batches[0].Rows {
				if row.TableID == TABLE_BLOCKS {
					blocksInDump++
				}
			}
		}
		if blocksInDump > 0 && *flagBlocksPerBatch > blocksInDump {
			n := (*flagBlocksPerBatch + blocksInDump - 1) / blocksInDump
			batches = mergeDumpBatches(batches, n)
			actualBlocks := blocksInDump * n
			fmt.Printf("  merged %d dump-batches → %d batches of ~%d blocks\n", n, len(batches), actualBlocks)
		} else {
			fmt.Printf("  blocks-per-batch=%d ≤ dump-batch-size=%d, no merge needed\n", *flagBlocksPerBatch, blocksInDump)
		}
	}
	fmt.Println()

	printDumpStats(batches)

	if *flagAnalyze {
		return
	}

	// Count actual blocks from dump (TABLE_BLOCKS rows = one per block)
	blocks := 0
	for _, b := range batches {
		for _, row := range b.Rows {
			if row.TableID == TABLE_BLOCKS {
				blocks++
			}
		}
	}

	if *flagTrunc {
		if err := truncateTables(*flagHost, *flagKS, *flagUser, *flagPass); err != nil {
			fmt.Fprintf(os.Stderr, "truncate error: %v\n", err)
			os.Exit(1)
		}
	}

	switch *flagMode {
	case "batch":
		fmt.Printf("Mode: saveBatch  split=%s  batch-split=%s  total-conns=%d\n\n",
			*flagSplit, *flagBatchSplit, total)

		pool, err := newPool(split, *flagHost, *flagKS, *flagUser, *flagPass)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pool error: %v\n", err)
			os.Exit(1)
		}
		defer pool.close()

		var errCount atomic.Int64
		var firstErr atomic.Value
		var written int64

		t0 := time.Now()
		for i := range batches {
			written += pool.saveBatch(&batches[i], batchSizes, &errCount, &firstErr)
		}
		elapsed := time.Since(t0)

		if errCount.Load() > 0 {
			fmt.Fprintf(os.Stderr, "  CQL errors: %d\n", errCount.Load())
		}
		rps := float64(written) / elapsed.Seconds()
		msPerBlock := elapsed.Seconds() * 1000 / float64(blocks)
		fmt.Printf("split=%-18s elapsed=%-10s rows/s=%-10.0f %.2f ms/block\n",
			*flagSplit, elapsed.Round(time.Millisecond), rps, msPerBlock)
		if v := firstErr.Load(); v != nil {
			fmt.Printf("  first CQL error: %s\n", v.(string))
		}

	case "firehose":
		fmt.Printf("Mode: firehose   split=%s  batch-split=%s  total-conns=%d\n\n",
			*flagSplit, *flagBatchSplit, total)

		elapsed, rows, firstErr, err := runFirehose(batches, split, batchSizes,
			*flagHost, *flagKS, *flagUser, *flagPass)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bench error: %v\n", err)
			os.Exit(1)
		}
		rps := float64(rows) / elapsed.Seconds()
		msPerBlock := elapsed.Seconds() * 1000 / float64(blocks)
		fmt.Printf("split=%-18s elapsed=%-10s rows/s=%-10.0f %.2f ms/block\n",
			*flagSplit, elapsed.Round(time.Millisecond), rps, msPerBlock)
		if firstErr != "" {
			fmt.Printf("  first CQL error: %s\n", firstErr)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %q (use batch or firehose)\n", *flagMode)
		os.Exit(1)
	}
}
