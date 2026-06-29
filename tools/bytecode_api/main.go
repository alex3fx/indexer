package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gocql/gocql"
)

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
	cluster.NumConns = 4

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to connect to Scylla: %v", err)
	}
	defer session.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/bytecode", handleBytecode)
	mux.HandleFunc("/same", handleSame)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	log.Printf("bytecode-api listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

// GET  /bytecode?address=0x...   → deployed bytecode hex for that address
// POST /bytecode  body: {"address":"0x..."}
func handleBytecode(w http.ResponseWriter, r *http.Request) {
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

// GET  /same?address=0x...    → all contracts with same deployed bytecode
// GET  /same?bytecode=0x...   → same, provide bytecode directly
// POST /same  body: {"address":"0x..."} or {"bytecode":"0x..."}
func handleSame(w http.ResponseWriter, r *http.Request) {
	var bytecodeHex string

	addr, bytecodeParam, err := parseSameParams(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if addr != "" {
		// Resolve address → deployed bytecode first
		err = session.Query(
			`SELECT deployed_bytecode FROM contracts_by_addresses WHERE address = ?`,
			addr,
		).Scan(&bytecodeHex)
		if err == gocql.ErrNotFound {
			jsonError(w, "contract not found", http.StatusNotFound)
			return
		}
		if err != nil {
			jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		bytecodeHex = bytecodeParam
	}

	hash, err := hashBytecodeHex(bytecodeHex)
	if err != nil {
		jsonError(w, fmt.Sprintf("invalid bytecode: %v", err), http.StatusBadRequest)
		return
	}

	iter := session.Query(
		`SELECT address FROM contracts_by_bytecode_hash WHERE bytecode_hash = ?`,
		hash,
	).Iter()

	var addresses []string
	var a string
	for iter.Scan(&a) {
		addresses = append(addresses, a)
	}
	if err := iter.Close(); err != nil {
		jsonError(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}

	if addresses == nil {
		addresses = []string{}
	}

	jsonResponse(w, map[string]interface{}{
		"bytecode_hash": "0x" + hex.EncodeToString(hash),
		"count":         len(addresses),
		"addresses":     addresses,
	})
}

// hashBytecodeHex decodes a hex bytecode string and returns its SHA256 hash.
func hashBytecodeHex(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(strings.TrimSpace(hexStr), "0x")
	hexStr = strings.TrimPrefix(hexStr, "0X")
	if hexStr == "" {
		return nil, fmt.Errorf("empty bytecode")
	}
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return h[:], nil
}

func normalizeAddr(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

// parseAddressParam reads "address" from GET params or POST JSON body.
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

// parseSameParams returns (address, bytecodeHex, err). Exactly one of address/bytecodeHex is non-empty.
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
	addr := normalizeAddr(body.Address)
	if addr != "" {
		return addr, "", nil
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
