package main

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

const numBuckets = 256
const sourceChunkSize = 512 * 1024

var session *gocql.Session

func main() {
	host := flag.String("host", "100.64.0.4", "Scylla host")
	port := flag.Int("port", 9042, "Scylla port")
	user := flag.String("user", "reader", "Scylla username")
	pass := flag.String("pass", "", "Scylla password")
	keyspace := flag.String("keyspace", "eth", "Scylla keyspace")
	listen := flag.String("listen", "0.0.0.0:8080", "HTTP listen address")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass is required")
	}

	cluster := gocql.NewCluster(*host)
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *user,
		Password: *pass,
	}
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = 8

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to connect to Scylla: %v", err)
	}
	defer session.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/bytecode", handleBytecode)
	mux.HandleFunc("/same", handleSame)
	mux.HandleFunc("/contract", handleContract)
	mux.HandleFunc("/verify", handleVerify)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	log.Printf("bytecode-api v2 listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

// ─── chain_id guard ──────────────────────────────────────────────────────────

// checkChainID enforces ?chain_id=1 (or absent, defaulting to ETH mainnet).
// Any other value returns HTTP 501. Works for both GET (query param) and
// POST (query param takes precedence; body is not consumed here).
func checkChainID(w http.ResponseWriter, r *http.Request) bool {
	v := r.URL.Query().Get("chain_id")
	if v == "" || v == "1" {
		return true
	}
	jsonError(w, fmt.Sprintf("chain_id=%s is not implemented yet", v), http.StatusNotImplemented)
	return false
}

// ─── /bytecode ───────────────────────────────────────────────────────────────

// GET /bytecode?address=0x...  → deployed bytecode hex (legacy compat, old table)
func handleBytecode(w http.ResponseWriter, r *http.Request) {
	if !checkChainID(w, r) {
		return
	}
	addr, err := parseAddressParam(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var deployedBytecode string
	err = session.Query(
		`SELECT deployed_bytecode FROM contracts_by_addresses WHERE address = ?`,
		addr,
	).Scan(&deployedBytecode)

	if err == gocql.ErrNotFound {
		jsonError(w, "contract not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}

	jsonResponse(w, map[string]string{
		"address":  addr,
		"bytecode": deployedBytecode,
	})
}

// ─── /contract ───────────────────────────────────────────────────────────────

// GET /contract?address=0x...
// Returns full contract info from v2 tables: bytecode identity, size, verified status, ABI.
func handleContract(w http.ResponseWriter, r *http.Request) {
	if !checkChainID(w, r) {
		return
	}
	addr, err := parseAddressParam(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, blockNum, txHash, deployer, err := resolveAddressV2(addr)
	if err == gocql.ErrNotFound {
		jsonError(w, "contract not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}

	var size int
	var kind int8
	var verified bool
	var verifiedAt time.Time
	var abiBlob []byte
	var sourceRef string
	err = session.Query(
		`SELECT size, kind, verified, verified_at, abi, source_ref FROM bytecode_store_v2 WHERE hash = ? AND seq = ?`,
		id.hash[:], id.seq,
	).Scan(&size, &kind, &verified, &verifiedAt, &abiBlob, &sourceRef)
	if err != nil && err != gocql.ErrNotFound {
		jsonError(w, fmt.Sprintf("bytecode lookup error: %v", err), http.StatusInternalServerError)
		return
	}

	var abiJSON string
	if len(abiBlob) > 0 {
		dec, e := zlibDecompress(abiBlob)
		if e == nil {
			abiJSON = string(dec)
		}
	}

	kindStr := "deployed"
	if kind == 1 {
		kindStr = "creation"
	}

	resp := map[string]interface{}{
		"address":       addr,
		"block_number":  blockNum,
		"tx_hash":       txHash,
		"deployer":      deployer,
		"bytecode_hash": "0x" + hex.EncodeToString(id.hash[:]),
		"bytecode_seq":  id.seq,
		"size":          size,
		"kind":          kindStr,
		"verified":      verified,
	}
	if verified {
		resp["verified_at"] = verifiedAt.Format(time.RFC3339)
	}
	if abiJSON != "" {
		resp["abi"] = json.RawMessage(abiJSON)
	}
	if sourceRef != "" {
		resp["source_ref"] = sourceRef
	}

	jsonResponse(w, resp)
}

// ─── /same ───────────────────────────────────────────────────────────────────

// GET  /same?address=0x...    → all contracts with same deployed bytecode
// GET  /same?bytecode=0x...   → same, provide raw bytecode hex directly
// POST /same  body: {"address":"0x..."} or {"bytecode":"0x..."}
//
// Uses v2 tables: contracts_by_address_v2 + addresses_by_bytecode (256 buckets, parallel).
// Filters out CREATE2-redeployed addresses whose current bytecode differs.
func handleSame(w http.ResponseWriter, r *http.Request) {
	if !checkChainID(w, r) {
		return
	}
	addr, bytecodeParam, err := parseSameParams(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var id bytecodeID

	if addr != "" {
		id, _, _, _, err = resolveAddressV2(addr)
		if err == gocql.ErrNotFound {
			jsonError(w, "contract not found", http.StatusNotFound)
			return
		}
		if err != nil {
			jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		raw, e := decodeHex(bytecodeParam)
		if e != nil {
			jsonError(w, fmt.Sprintf("invalid bytecode: %v", e), http.StatusBadRequest)
			return
		}
		h := sha256.Sum256(raw)
		id = bytecodeID{hash: h, seq: 0}
	}

	addresses, err := fetchAllAddresses(id)
	if err != nil {
		jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}

	// Filter out addresses redeployed with a different bytecode (CREATE2+selfdestruct).
	addresses = filterCurrentBytecode(addresses, id)

	jsonResponse(w, map[string]interface{}{
		"bytecode_hash": "0x" + hex.EncodeToString(id.hash[:]),
		"bytecode_seq":  id.seq,
		"count":         len(addresses),
		"addresses":     addresses,
	})
}

// fetchAllAddresses scans all 256 buckets of addresses_by_bytecode in parallel.
func fetchAllAddresses(id bytecodeID) ([]string, error) {
	type result struct {
		addrs []string
		err   error
	}
	results := make([]result, numBuckets)
	var wg sync.WaitGroup
	wg.Add(numBuckets)
	for b := 0; b < numBuckets; b++ {
		b := b
		go func() {
			defer wg.Done()
			iter := session.Query(
				`SELECT address FROM addresses_by_bytecode WHERE hash = ? AND seq = ? AND bucket = ?`,
				id.hash[:], id.seq, int16(b),
			).Iter()
			var a string
			var addrs []string
			for iter.Scan(&a) {
				addrs = append(addrs, a)
			}
			if err := iter.Close(); err != nil {
				results[b] = result{err: err}
				return
			}
			results[b] = result{addrs: addrs}
		}()
	}
	wg.Wait()

	var all []string
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		all = append(all, r.addrs...)
	}
	return all, nil
}

// filterCurrentBytecode removes addresses whose latest deployment has a different bytecode.
// Runs spot-checks concurrently, up to 64 at a time.
func filterCurrentBytecode(addrs []string, want bytecodeID) []string {
	type check struct {
		addr string
		keep bool
	}
	sem := make(chan struct{}, 64)
	out := make([]check, len(addrs))
	var wg sync.WaitGroup
	for i, a := range addrs {
		wg.Add(1)
		i, a := i, a
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			cur, _, _, _, err := resolveAddressV2(a)
			if err != nil {
				// On error keep the address (conservative).
				out[i] = check{addr: a, keep: true}
				return
			}
			out[i] = check{addr: a, keep: cur.hash == want.hash && cur.seq == want.seq}
		}()
	}
	wg.Wait()

	var kept []string
	for _, c := range out {
		if c.keep {
			kept = append(kept, c.addr)
		}
	}
	return kept
}

// ─── /verify ─────────────────────────────────────────────────────────────────

// POST /verify
// Body: {"address":"0x...", "abi":"[...]", "source":"<hex or base64 of source archive>"}
// Marks the bytecode as verified in bytecode_store_v2, stores ABI + source,
// returns the list of all clone addresses sharing that bytecode.
func handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if !checkChainID(w, r) {
		return
	}

	var body struct {
		Address string `json:"address"`
		ABI     string `json:"abi"`     // JSON array/string
		Source  string `json:"source"`  // hex-encoded source archive bytes
	}
	if err := decodeJSONBody(r, &body); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	addr := normalizeAddr(body.Address)
	if addr == "" {
		jsonError(w, "address required", http.StatusBadRequest)
		return
	}

	// Resolve address → BytecodeId.
	id, _, _, _, err := resolveAddressV2(addr)
	if err == gocql.ErrNotFound {
		// Contract not indexed yet — store in pending_verifications.
		abiBlob := []byte(body.ABI)
		srcBlob, _ := decodeHex(body.Source)
		_ = session.Query(
			`INSERT INTO pending_verifications (address, source, abi, received_at) VALUES (?, ?, ?, ?)`,
			addr, srcBlob, abiBlob, time.Now(),
		).Exec()
		jsonResponse(w, map[string]string{"status": "pending", "address": addr})
		return
	}
	if err != nil {
		jsonError(w, fmt.Sprintf("address lookup error: %v", err), http.StatusInternalServerError)
		return
	}

	// Check current verified status.
	var alreadyVerified bool
	_ = session.Query(
		`SELECT verified FROM bytecode_store_v2 WHERE hash = ? AND seq = ?`,
		id.hash[:], id.seq,
	).Scan(&alreadyVerified)

	if alreadyVerified {
		addresses, _ := fetchAllAddresses(id)
		addresses = filterCurrentBytecode(addresses, id)
		jsonResponse(w, map[string]interface{}{
			"status":        "already_verified",
			"bytecode_hash": "0x" + hex.EncodeToString(id.hash[:]),
			"bytecode_seq":  id.seq,
			"address_count": len(addresses),
			"addresses":     addresses,
		})
		return
	}

	// Compress ABI with zlib.
	var abiBlob []byte
	if body.ABI != "" {
		abiBlob, err = zlibCompress([]byte(body.ABI))
		if err != nil {
			jsonError(w, fmt.Sprintf("abi compress error: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Store source in source_store (chunked at 512KB).
	sourceRef := ""
	if body.Source != "" {
		srcBytes, e := decodeHex(body.Source)
		if e != nil {
			// Try as raw bytes if not hex.
			srcBytes = []byte(body.Source)
		}
		if err := storeSource(id, srcBytes); err != nil {
			log.Printf("[verify] source store error addr=%s: %v", addr, err)
		} else {
			sourceRef = "source_store"
		}
	}

	// Mark bytecode as verified.
	now := time.Now()
	err = session.Query(
		`UPDATE bytecode_store_v2 SET verified = true, verified_at = ?, verified_via_address = ?, abi = ?, source_ref = ? WHERE hash = ? AND seq = ?`,
		now, addr, abiBlob, sourceRef, id.hash[:], id.seq,
	).Exec()
	if err != nil {
		jsonError(w, fmt.Sprintf("update error: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[verify] verified bytecode hash=%x seq=%d via addr=%s", id.hash, id.seq, addr)

	// Collect all clone addresses.
	addresses, err := fetchAllAddresses(id)
	if err != nil {
		log.Printf("[verify] fetchAllAddresses error: %v", err)
		addresses = []string{addr}
	}
	addresses = filterCurrentBytecode(addresses, id)

	jsonResponse(w, map[string]interface{}{
		"status":        "verified",
		"bytecode_hash": "0x" + hex.EncodeToString(id.hash[:]),
		"bytecode_seq":  id.seq,
		"verified_at":   now.Format(time.RFC3339),
		"address_count": len(addresses),
		"addresses":     addresses,
	})
}

// storeSource writes srcBytes into source_store in 512KB chunks.
func storeSource(id bytecodeID, srcBytes []byte) error {
	chunk := 0
	for len(srcBytes) > 0 {
		end := sourceChunkSize
		if end > len(srcBytes) {
			end = len(srcBytes)
		}
		err := session.Query(
			`INSERT INTO source_store (hash, seq, chunk, data) VALUES (?, ?, ?, ?)`,
			id.hash[:], id.seq, chunk, srcBytes[:end],
		).Exec()
		if err != nil {
			return err
		}
		srcBytes = srcBytes[end:]
		chunk++
	}
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

type bytecodeID struct {
	hash [32]byte
	seq  int8
}

// resolveAddressV2 returns the latest BytecodeId for an address from contracts_by_address_v2.
func resolveAddressV2(addr string) (bytecodeID, int64, string, string, error) {
	var hashBlob []byte
	var seq int8
	var blockNum int64
	var txHash, deployer string
	err := session.Query(
		`SELECT bytecode_hash, bytecode_seq, block_number, tx_hash, deployer FROM contracts_by_address_v2 WHERE address = ? LIMIT 1`,
		addr,
	).Scan(&hashBlob, &seq, &blockNum, &txHash, &deployer)
	if err != nil {
		return bytecodeID{}, 0, "", "", err
	}
	var id bytecodeID
	copy(id.hash[:], hashBlob)
	id.seq = seq
	return id, blockNum, txHash, deployer, nil
}

func zlibCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zlibDecompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func decodeHex(s string) ([]byte, error) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "0x")
	s = strings.TrimPrefix(s, "0X")
	if s == "" {
		return nil, fmt.Errorf("empty hex string")
	}
	return hex.DecodeString(s)
}

func normalizeAddr(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

func parseAddressParam(r *http.Request) (string, error) {
	if r.Method == http.MethodGet {
		addr := normalizeAddr(r.URL.Query().Get("address"))
		if addr == "" {
			return "", fmt.Errorf("address parameter required")
		}
		return addr, nil
	}
	var body struct {
		Address string `json:"address"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		return "", err
	}
	addr := normalizeAddr(body.Address)
	if addr == "" {
		return "", fmt.Errorf("address field required")
	}
	return addr, nil
}

func parseSameParams(r *http.Request) (string, string, error) {
	if r.Method == http.MethodGet {
		addr := normalizeAddr(r.URL.Query().Get("address"))
		bc := r.URL.Query().Get("bytecode")
		if addr == "" && bc == "" {
			return "", "", fmt.Errorf("address or bytecode parameter required")
		}
		if addr != "" {
			return addr, "", nil
		}
		return "", bc, nil
	}
	var body struct {
		Address  string `json:"address"`
		Bytecode string `json:"bytecode"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		return "", "", err
	}
	if body.Address != "" {
		return normalizeAddr(body.Address), "", nil
	}
	if body.Bytecode != "" {
		return "", body.Bytecode, nil
	}
	return "", "", fmt.Errorf("address or bytecode field required")
}

func decodeJSONBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
	if err != nil {
		return fmt.Errorf("failed to read body: %v", err)
	}
	return json.Unmarshal(data, v)
}

func jsonResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
