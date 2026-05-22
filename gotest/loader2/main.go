// loader2 — realtime single-block write benchmark.
// Loads 100 pre-encoded blocks into memory, then sends one block's
// UNLOGGED BATCH to ScyllaDB every 100ms (simulating 10 blocks/sec).
// Measures CQL round-trip latency per block.
//
// Usage:
//
//	./loader2 -dump=dump_100_b1.bin -truncate
//	./loader2 -dump=dump_100_b1.bin -interval=100
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

// ─── Constants (same as loader) ──────────────────────────────────────────────

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
	OP_BATCH     = 0x0D
	OP_READY     = 0x02
	OP_AUTH      = 0x03
	OP_AUTH_OK   = 0x10
	OP_RESULT    = 0x08
	OP_ERROR     = 0x00
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

// ─── Flags ────────────────────────────────────────────────────────────────────

var (
	flagDump     = flag.String("dump", "dump_100_b1.bin", "Dump file (1 block per batch)")
	flagHost     = flag.String("host", "172.31.208.104:9142", "ScyllaDB host:port")
	flagKS       = flag.String("ks", "eth", "Keyspace")
	flagUser     = flag.String("user", "cassandra", "Username")
	flagPass     = flag.String("pass", "cassandra", "Password")
	flagInterval = flag.Int("interval", 100, "Send interval in ms (default 100 = 10 blocks/sec)")
	flagBatch    = flag.Int("batch", 100, "Rows per UNLOGGED BATCH frame")
	flagTrunc    = flag.Bool("truncate", false, "TRUNCATE tables before benchmark")
)

// ─── CQL connection ───────────────────────────────────────────────────────────

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

const OP_EVENT = 0x0C

func (c *CQLConn) recvFrame() (op byte, body []byte, err error) {
	for {
		hdr := make([]byte, 9)
		if _, err = io.ReadFull(c.r, hdr); err != nil {
			return
		}
		op = hdr[4]
		n := binary.BigEndian.Uint32(hdr[5:])
		if n > 0 {
			body = make([]byte, n)
			if _, err = io.ReadFull(c.r, body); err != nil {
				return
			}
		} else {
			body = nil
		}
		if op == OP_EVENT {
			// Skip unsolicited server events (topology/schema changes)
			continue
		}
		return
	}
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
		if op, b, err = c.recvFrame(); err != nil {
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
	op, b, err = c.recvFrame()
	if err != nil {
		return err
	}
	if op == OP_ERROR {
		return cqlErr(b)
	}
	return nil
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
		if op == OP_ERROR {
			return nil, fmt.Errorf("prepare CQL error: %w", cqlErr(b))
		}
		return nil, fmt.Errorf("prepare unexpected op=%02x body=%x", op, b)
	}
	idLen := binary.BigEndian.Uint16(b[4:6])
	return b[6 : 6+idLen], nil
}

func (c *CQLConn) sendBatch(prepID []byte, nvals uint16, rowVals [][]byte) error {
	if len(rowVals) == 0 {
		return nil
	}
	body := make([]byte, 0, 256*1024)
	body = append(body, 0x01) // UNLOGGED
	body = append(body, byte(len(rowVals)>>8), byte(len(rowVals)))
	for _, rv := range rowVals {
		body = append(body, 0x01)
		body = append(body, byte(len(prepID)>>8), byte(len(prepID)))
		body = append(body, prepID...)
		body = append(body, byte(nvals>>8), byte(nvals))
		body = append(body, rv...)
	}
	body = append(body, 0x00, 0x01, 0x00) // consistency ONE, flags=0
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
	op, b, err := c.recvFrame()
	_ = b
	if err != nil {
		return err
	}
	if op == OP_ERROR {
		return cqlErr(b)
	}
	return nil
}

// ─── Dump loading ─────────────────────────────────────────────────────────────

// BlockBatch holds all rows for one block, split by table.
type BlockBatch struct {
	ID       uint32
	Tables   [TABLE_COUNT][][]byte // per-table row values
	RowCount [TABLE_COUNT]int
}

func loadDump(path string) ([]BlockBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	// Verify magic
	magic := make([]byte, len(MAGIC))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != MAGIC {
		return nil, fmt.Errorf("bad magic: %x", magic)
	}

	var batches []BlockBatch
	hdr := make([]byte, 10)
	for {
		if _, err := io.ReadFull(r, hdr[:1]); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if hdr[0] != BATCH_MARKER {
			return nil, fmt.Errorf("expected batch marker, got %02x", hdr[0])
		}
		if _, err := io.ReadFull(r, hdr[1:10]); err != nil {
			return nil, err
		}
		// hdr[1] = 0x00, hdr[2..5] = batch_id BE, hdr[6..9] = total_rows BE
		batchID := binary.BigEndian.Uint32(hdr[2:6])
		totalRows := binary.BigEndian.Uint32(hdr[6:10])

		bb := BlockBatch{ID: batchID}
		rowHdr := make([]byte, 7)
		for i := uint32(0); i < totalRows; i++ {
			if _, err := io.ReadFull(r, rowHdr); err != nil {
				return nil, fmt.Errorf("row hdr: %w", err)
			}
			tableID := rowHdr[0]
			nvals := binary.BigEndian.Uint16(rowHdr[1:3])
			vlen := binary.BigEndian.Uint32(rowHdr[3:7])
			_ = nvals
			vals := make([]byte, vlen)
			if _, err := io.ReadFull(r, vals); err != nil {
				return nil, fmt.Errorf("row vals: %w", err)
			}
			if int(tableID) < TABLE_COUNT {
				bb.Tables[tableID] = append(bb.Tables[tableID], vals)
				bb.RowCount[tableID]++
			}
		}
		batches = append(batches, bb)
	}
	return batches, nil
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	fmt.Printf("loader2: single-block realtime write benchmark\n")
	fmt.Printf("  dump:     %s\n", *flagDump)
	fmt.Printf("  host:     %s\n", *flagHost)
	fmt.Printf("  interval: %dms (%.1f blocks/sec)\n", *flagInterval, 1000.0/float64(*flagInterval))
	fmt.Printf("  batch:    %d rows/frame\n\n", *flagBatch)

	// ── Load dump ────────────────────────────────────────────────────────────
	fmt.Printf("Loading dump... ")
	batches, err := loadDump(*flagDump)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load dump: %v\n", err)
		os.Exit(1)
	}
	totalRows := 0
	for _, b := range batches {
		for _, rc := range b.RowCount {
			totalRows += rc
		}
	}
	fmt.Printf("%d blocks, %d total rows (avg %.0f rows/block)\n\n",
		len(batches), totalRows, float64(totalRows)/float64(len(batches)))

	// ── Connect & prepare ────────────────────────────────────────────────────
	fmt.Printf("Connecting to %s ...\n", *flagHost)
	conns := [TABLE_COUNT]*CQLConn{}
	prepIDs := [TABLE_COUNT][]byte{}
	tableNames := [TABLE_COUNT]string{"blocks", "txs", "logs", "itxs", "contracts", "cba"}
	for t := 0; t < TABLE_COUNT; t++ {
		c, err := newCQLConn(*flagHost, *flagKS, *flagUser, *flagPass)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect table=%d (%s): %v\n", t, tableNames[t], err)
			os.Exit(1)
		}
		conns[t] = c
		pid, err := c.prepare(insertStmts[t])
		if err != nil {
			fmt.Fprintf(os.Stderr, "prepare table=%d (%s): %v\n", t, tableNames[t], err)
			os.Exit(1)
		}
		prepIDs[t] = pid
		fmt.Printf("  table=%d (%s) prepared, id_len=%d\n", t, tableNames[t], len(pid))
	}
	defer func() {
		for t := 0; t < TABLE_COUNT; t++ {
			if conns[t] != nil {
				conns[t].close()
			}
		}
	}()
	fmt.Printf("Connected (6 connections, 1 per table).\n\n")

	// ── Truncate ─────────────────────────────────────────────────────────────
	if *flagTrunc {
		fmt.Printf("Truncating tables... ")
		tables := []string{"blocks", "transactions", "logs", "internal_transactions", "contracts", "contracts_by_addresses"}
		for i, tbl := range tables {
			if err := conns[i].query("TRUNCATE eth." + tbl); err != nil {
				fmt.Fprintf(os.Stderr, "truncate %s: %v\n", tbl, err)
				os.Exit(1)
			}
		}
		fmt.Printf("done.\n\n")
	}

	// ── Benchmark ─────────────────────────────────────────────────────────────
	batchSize := *flagBatch
	interval := time.Duration(*flagInterval) * time.Millisecond
	n := len(batches)

	saveTimes := make([]float64, 0, n)
	tableTimes := [TABLE_COUNT][]float64{}
	for t := range tableTimes {
		tableTimes[t] = make([]float64, 0, n)
	}

	fmt.Printf("%-8s %8s %8s %8s %8s %8s %8s | %8s | rows\n",
		"block", "blocks", "txs", "logs", "itxs", "conts", "cba", "save_ms")
	fmt.Printf("%s\n", "─────────────────────────────────────────────────────────────────────────")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i, bb := range batches {
		<-ticker.C

		t0 := time.Now()

		// Send all 6 table batches in parallel, each on its own connection
		var wg sync.WaitGroup
		errs := [TABLE_COUNT]error{}
		tms := [TABLE_COUNT]float64{}

		for t := 0; t < TABLE_COUNT; t++ {
			wg.Add(1)
			go func(t int) {
				defer wg.Done()
				rows := bb.Tables[t]
				if len(rows) == 0 {
					return
				}
				ts := time.Now()
				// Send in chunks of batchSize
				for off := 0; off < len(rows); off += batchSize {
					end := off + batchSize
					if end > len(rows) {
						end = len(rows)
					}
					if e := conns[t].sendBatch(prepIDs[t], nValues[t], rows[off:end]); e != nil {
						errs[t] = e
						return
					}
				}
				tms[t] = float64(time.Since(ts).Microseconds()) / 1000.0
			}(t)
		}
		wg.Wait()

		saveMs := float64(time.Since(t0).Microseconds()) / 1000.0
		saveTimes = append(saveTimes, saveMs)
		for t := range tableTimes {
			tableTimes[t] = append(tableTimes[t], tms[t])
		}

		errStr := ""
		for t, e := range errs {
			if e != nil {
				errStr += fmt.Sprintf(" ERR[%d]=%v", t, e)
			}
		}

		totalRowsBlock := 0
		for _, rc := range bb.RowCount {
			totalRowsBlock += rc
		}

		fmt.Printf("%-8d %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f | %8.1f | %d%s\n",
			i+1,
			tms[TABLE_BLOCKS], tms[TABLE_TXS], tms[TABLE_LOGS],
			tms[TABLE_ITXS], tms[TABLE_CONTS], tms[TABLE_CBA],
			saveMs, totalRowsBlock, errStr)
	}

	// ── Summary ───────────────────────────────────────────────────────────────
	fmt.Printf("\n%s\n", "═══════════════════════════════════════════════════════════════════════════")
	fmt.Printf("loader2 summary (%d blocks, interval=%dms)\n", n, *flagInterval)
	fmt.Printf("%s\n\n", "═══════════════════════════════════════════════════════════════════════════")

	printStats := func(label string, vals []float64) {
		if len(vals) == 0 {
			return
		}
		sorted := make([]float64, len(vals))
		copy(sorted, vals)
		sort.Float64s(sorted)
		sum := 0.0
		for _, v := range sorted {
			sum += v
		}
		avg := sum / float64(len(sorted))
		mn := sorted[0]
		mx := sorted[len(sorted)-1]
		p50 := sorted[len(sorted)*50/100]
		p95 := sorted[len(sorted)*95/100]
		p99 := sorted[int(math.Min(float64(len(sorted)-1), float64(len(sorted)*99/100)))]
		fmt.Printf("  %-8s avg=%6.1fms  min=%5.1fms  p50=%5.1fms  p95=%5.1fms  p99=%5.1fms  max=%5.1fms\n",
			label, avg, mn, p50, p95, p99, mx)
	}

	printStats("save", saveTimes)
	fmt.Println()
	printStats("blocks", tableTimes[TABLE_BLOCKS])
	printStats("txs", tableTimes[TABLE_TXS])
	printStats("logs", tableTimes[TABLE_LOGS])
	printStats("itxs", tableTimes[TABLE_ITXS])
	printStats("conts", tableTimes[TABLE_CONTS])
	printStats("cba", tableTimes[TABLE_CBA])
}
