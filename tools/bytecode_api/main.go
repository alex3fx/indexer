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
var apiKey string

func main() {
	host     := flag.String("host", "100.64.0.4", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "reader", "Scylla username")
	pass     := flag.String("pass", "", "Scylla password")
	keyspace := flag.String("keyspace", "eth", "Scylla keyspace")
	listen   := flag.String("listen", "0.0.0.0:8080", "HTTP listen address")
	key      := flag.String("api-key", "354bf5a9-a29a-4879-9f3d-d3c09c6a610a", "Required Api-Access-Key header value")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass is required")
	}
	apiKey = *key

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
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/same",     auth(handleSame))
	mux.HandleFunc("/contract", auth(handleContract))
	mux.HandleFunc("/verify",   auth(handleVerify))

	log.Printf("bytecode-api v5 listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

// ─── auth middleware ──────────────────────────────────────────────────────────

func auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Api-Access-Key") != apiKey {
			jsonError(w, "missing or invalid Api-Access-Key header", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// ─── chain_id guard ──────────────────────────────────────────────────────────

func checkChainID(w http.ResponseWriter, r *http.Request) bool {
	v := r.URL.Query().Get("chain_id")
	if v == "" || v == "1" {
		return true
	}
	jsonError(w, fmt.Sprintf("chain_id=%s is not implemented yet", v), http.StatusNotImplemented)
	return false
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

	deployed, creation, blockNum, txHash, deployer, err := resolveAddressV2(addr)
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
		deployed.hash[:], deployed.seq,
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

	resp := map[string]interface{}{
		"address":                addr,
		"block_number":           blockNum,
		"tx_hash":                txHash,
		"deployer":               deployer,
		"deployed_bytecode_hash": "0x" + hex.EncodeToString(deployed.hash[:]),
		"deployed_bytecode_seq":  deployed.seq,
		"creation_bytecode_hash": "0x" + hex.EncodeToString(creation.hash[:]),
		"creation_bytecode_seq":  creation.seq,
		"size":                   size,
		"verified":               verified,
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

// GET  /same?address=0x...                  → all contracts with same deployed bytecode
// GET  /same?deployed_bytecode=0x...        → same, provide raw deployed (runtime) bytecode hex
// GET  /same?creation_bytecode=0x...        → not yet implemented (no reverse index)
// POST /same  body: {"address":"0x..."} or {"deployed_bytecode":"0x..."} or {"creation_bytecode":"0x..."}
//
// Deployed bytecode (runtime code) is what eth_getCode returns and what actually executes.
// Creation bytecode (init code) is the data field of the deploy tx; it includes the constructor.
// Only deployed bytecode has a reverse index (addresses_by_bytecode).
func handleSame(w http.ResponseWriter, r *http.Request) {
	if !checkChainID(w, r) {
		return
	}
	addr, deployedHex, creationHex, err := parseSameParams(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// creation_bytecode search: no reverse index exists yet.
	if creationHex != "" {
		jsonError(w,
			"creation_bytecode search is not yet implemented: there is no reverse index for creation bytecode. "+
				"Use deployed_bytecode= to search by runtime code (what eth_getCode returns), "+
				"or address= to look up a specific contract.",
			http.StatusNotImplemented,
		)
		return
	}

	var id bytecodeID

	if addr != "" {
		deployed, _, _, _, _, err := resolveAddressV2(addr)
		if err == gocql.ErrNotFound {
			jsonError(w, "contract not found", http.StatusNotFound)
			return
		}
		if err != nil {
			jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
			return
		}
		id = deployed
	} else {
		raw, e := decodeHex(deployedHex)
		if e != nil {
			jsonError(w, fmt.Sprintf("invalid deployed_bytecode: %v", e), http.StatusBadRequest)
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

	addresses = filterCurrentBytecode(addresses, id)

	jsonResponse(w, map[string]interface{}{
		"deployed_bytecode_hash": "0x" + hex.EncodeToString(id.hash[:]),
		"deployed_bytecode_seq":  id.seq,
		"count":                  len(addresses),
		"addresses":              addresses,
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
			cur, _, _, _, _, err := resolveAddressV2(a)
			if err != nil {
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
// Body: {"address":"0x...", "abi":"[...]", "source":"<hex of source archive>"}
// ABI and source must be provided together — one without the other is rejected.
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
		ABI     string `json:"abi"`    // JSON array/string
		Source  string `json:"source"` // hex-encoded source archive bytes
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

	// ABI and source must always come together.
	if body.ABI != "" && body.Source == "" {
		jsonError(w, "source is required when abi is provided: ABI alone without source code is not accepted", http.StatusBadRequest)
		return
	}
	if body.Source != "" && body.ABI == "" {
		jsonError(w, "abi is required when source is provided", http.StatusBadRequest)
		return
	}

	// Resolve address → BytecodeId.
	deployed, _, _, _, _, err := resolveAddressV2(addr)
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
		deployed.hash[:], deployed.seq,
	).Scan(&alreadyVerified)

	if alreadyVerified {
		addresses, _ := fetchAllAddresses(deployed)
		addresses = filterCurrentBytecode(addresses, deployed)
		jsonResponse(w, map[string]interface{}{
			"status":                 "already_verified",
			"deployed_bytecode_hash": "0x" + hex.EncodeToString(deployed.hash[:]),
			"deployed_bytecode_seq":  deployed.seq,
			"address_count":          len(addresses),
			"addresses":              addresses,
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
			srcBytes = []byte(body.Source)
		}
		if err := storeSource(deployed, srcBytes); err != nil {
			log.Printf("[verify] source store error addr=%s: %v", addr, err)
		} else {
			sourceRef = "source_store"
		}
	}

	// Mark bytecode as verified.
	now := time.Now()
	err = session.Query(
		`UPDATE bytecode_store_v2 SET verified = true, verified_at = ?, verified_via_address = ?, abi = ?, source_ref = ? WHERE hash = ? AND seq = ?`,
		now, addr, abiBlob, sourceRef, deployed.hash[:], deployed.seq,
	).Exec()
	if err != nil {
		jsonError(w, fmt.Sprintf("update error: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[verify] verified bytecode hash=%x seq=%d via addr=%s", deployed.hash, deployed.seq, addr)

	addresses, err := fetchAllAddresses(deployed)
	if err != nil {
		log.Printf("[verify] fetchAllAddresses error: %v", err)
		addresses = []string{addr}
	}
	addresses = filterCurrentBytecode(addresses, deployed)

	jsonResponse(w, map[string]interface{}{
		"status":                 "verified",
		"deployed_bytecode_hash": "0x" + hex.EncodeToString(deployed.hash[:]),
		"deployed_bytecode_seq":  deployed.seq,
		"verified_at":            now.Format(time.RFC3339),
		"address_count":          len(addresses),
		"addresses":              addresses,
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

// resolveAddressV2 returns the deployed and creation BytecodeIds for an address.
// Uses LIMIT 1 to get the latest deployment (highest block_number by Scylla clustering).
func resolveAddressV2(addr string) (deployed, creation bytecodeID, blockNum int64, txHash, deployer string, err error) {
	var deployedHash, creationHash []byte
	var deployedSeq, creationSeq int8
	err = session.Query(
		`SELECT bytecode_hash, bytecode_seq, creation_hash, creation_seq, block_number, tx_hash, deployer
		   FROM contracts_by_address_v2 WHERE address = ? LIMIT 1`,
		addr,
	).Scan(&deployedHash, &deployedSeq, &creationHash, &creationSeq, &blockNum, &txHash, &deployer)
	if err != nil {
		return
	}
	copy(deployed.hash[:], deployedHash)
	deployed.seq = deployedSeq
	copy(creation.hash[:], creationHash)
	creation.seq = creationSeq
	return
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

func parseSameParams(r *http.Request) (addr, deployedHex, creationHex string, err error) {
	if r.Method == http.MethodGet {
		addr = normalizeAddr(r.URL.Query().Get("address"))
		deployedHex = r.URL.Query().Get("deployed_bytecode")
		creationHex = r.URL.Query().Get("creation_bytecode")
	} else {
		var body struct {
			Address         string `json:"address"`
			DeployedBytecode string `json:"deployed_bytecode"`
			CreationBytecode string `json:"creation_bytecode"`
		}
		if err = decodeJSONBody(r, &body); err != nil {
			return
		}
		addr = normalizeAddr(body.Address)
		deployedHex = body.DeployedBytecode
		creationHex = body.CreationBytecode
	}

	set := 0
	if addr != "" { set++ }
	if deployedHex != "" { set++ }
	if creationHex != "" { set++ }
	if set == 0 {
		err = fmt.Errorf("one of address, deployed_bytecode, or creation_bytecode is required")
		return
	}
	if set > 1 {
		err = fmt.Errorf("provide only one of address, deployed_bytecode, or creation_bytecode")
		return
	}
	return
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
