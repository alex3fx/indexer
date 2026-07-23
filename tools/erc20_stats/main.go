// erc20_stats: single-pass scan of eth.erc20_tokens, outputs:
//   - standard classification counts (fully/partially/minimally/not/null)
//   - per-method-flag counts (true/false/null)
//   - method-flag combination breakdown for partially_following_standard tokens
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type tristate int8

const (
	tsNull  tristate = 0
	tsFalse tristate = 1
	tsTrue  tristate = 2
)

func ts(v *bool) tristate {
	if v == nil {
		return tsNull
	}
	if *v {
		return tsTrue
	}
	return tsFalse
}

func tsStr(t tristate) string {
	switch t {
	case tsTrue:
		return "true"
	case tsFalse:
		return "false"
	default:
		return "null"
	}
}

type methodPattern struct {
	balanceOf    bool
	transfer     bool
	transferFrom bool
	approve      bool
	allowance    bool
}

func (p methodPattern) String() string {
	bit := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	return fmt.Sprintf("[%s%s%s%s%s] balanceOf=%s transfer=%s transferFrom=%s approve=%s allowance=%s",
		bit(p.balanceOf), bit(p.transfer), bit(p.transferFrom), bit(p.approve), bit(p.allowance),
		bit(p.balanceOf), bit(p.transfer), bit(p.transferFrom), bit(p.approve), bit(p.allowance),
	)
}

func main() {
	host     := flag.String("host", "127.0.0.1", "Scylla host")
	port     := flag.Int("port", 9042, "Scylla port")
	user     := flag.String("user", "cassandra", "user")
	pass     := flag.String("pass", "", "password")
	keyspace := flag.String("keyspace", "eth", "keyspace")
	pageSize := flag.Int("page-size", 5000, "CQL page size")
	timeout  := flag.Duration("timeout", 120*time.Second, "Scylla query timeout")
	retries  := flag.Int("retries", 5, "full-scan restart attempts on iterator error")
	flag.Parse()

	if *pass == "" {
		log.Fatal("--pass required")
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

	// classification counts: [fully, partially, minimally, not_following, null]
	classCount := map[string]int64{
		"fully":     0,
		"partially": 0,
		"minimally": 0,
		"not":       0,
		"null":      0,
	}

	// per-flag counts: flag name → [null, false, true]
	flagNames := []string{"has_balance_of", "has_transfer", "has_transfer_from", "has_approve", "has_allowance", "is_standard_decimals"}
	flagCount := make(map[string][3]int64) // index: tsNull=0, tsFalse=1, tsTrue=2
	for _, f := range flagNames {
		flagCount[f] = [3]int64{}
	}

	// method pattern breakdown for partially_following_standard tokens
	partialPatterns := make(map[methodPattern]int64)

	// method pattern breakdown for ALL tokens (for reference)
	allPatterns := make(map[methodPattern]int64)

	start := time.Now()
	var total int64
	var logEvery int64 = 500000

	var delay time.Duration = 2 * time.Second
	for attempt := 0; attempt < *retries; attempt++ {
		if attempt > 0 {
			log.Printf("[retry %d/%d] restarting full scan after error (delay=%s)", attempt+1, *retries, delay)
			time.Sleep(delay)
			if delay < 60*time.Second {
				delay *= 2
			}
			// reset counters for clean restart
			total = 0
			for k := range classCount {
				classCount[k] = 0
			}
			for _, fn := range flagNames {
				flagCount[fn] = [3]int64{}
			}
			for k := range partialPatterns {
				delete(partialPatterns, k)
			}
			for k := range allPatterns {
				delete(allPatterns, k)
			}
		}

		iter := session.Query(
			`SELECT has_balance_of, has_transfer, has_transfer_from, has_approve, has_allowance,
			        is_standard_decimals,
			        is_fully_following_standard, is_partially_following_standard,
			        is_minimally_following_standard, is_not_following_standard
			 FROM erc20_tokens`,
		).PageSize(*pageSize).Iter()

		var (
			hasBalanceOf    *bool
			hasTransfer     *bool
			hasTransferFrom *bool
			hasApprove      *bool
			hasAllowance    *bool
			isStdDecimals   *bool
			isFully         *bool
			isPartially     *bool
			isMinimally     *bool
			isNot           *bool
		)

		scanOK := true
		for iter.Scan(
			&hasBalanceOf, &hasTransfer, &hasTransferFrom, &hasApprove, &hasAllowance,
			&isStdDecimals,
			&isFully, &isPartially, &isMinimally, &isNot,
		) {
			total++
			if total%logEvery == 0 {
				log.Printf("[progress] scanned=%d elapsed=%.1fs", total, time.Since(start).Seconds())
			}

			// classification
			switch {
			case isFully != nil && *isFully:
				classCount["fully"]++
			case isPartially != nil && *isPartially:
				classCount["partially"]++
			case isMinimally != nil && *isMinimally:
				classCount["minimally"]++
			case isNot != nil && *isNot:
				classCount["not"]++
			default:
				classCount["null"]++
			}

			// per-flag
			for _, fn := range flagNames {
				var v *bool
				switch fn {
				case "has_balance_of":
					v = hasBalanceOf
				case "has_transfer":
					v = hasTransfer
				case "has_transfer_from":
					v = hasTransferFrom
				case "has_approve":
					v = hasApprove
				case "has_allowance":
					v = hasAllowance
				case "is_standard_decimals":
					v = isStdDecimals
				}
				arr := flagCount[fn]
				arr[ts(v)]++
				flagCount[fn] = arr
			}

			// method pattern
			pat := methodPattern{
				balanceOf:    hasBalanceOf != nil && *hasBalanceOf,
				transfer:     hasTransfer != nil && *hasTransfer,
				transferFrom: hasTransferFrom != nil && *hasTransferFrom,
				approve:      hasApprove != nil && *hasApprove,
				allowance:    hasAllowance != nil && *hasAllowance,
			}
			allPatterns[pat]++
			if isPartially != nil && *isPartially {
				partialPatterns[pat]++
			}
		}
		if err := iter.Close(); err != nil {
			log.Printf("[ERROR] scan attempt %d/%d failed at row %d: %v", attempt+1, *retries, total, err)
			scanOK = false
		}
		if scanOK {
			break
		}
		if attempt == *retries-1 {
			log.Fatalf("all %d scan attempts failed", *retries)
		}
	}

	elapsed := time.Since(start)

	fmt.Printf("\n=== erc20_tokens scan: %d rows in %.1fs ===\n\n", total, elapsed.Seconds())

	fmt.Println("=== Standard classification ===")
	fmt.Printf("  fully_following_standard:     %8d  (%.2f%%)\n", classCount["fully"], pct(classCount["fully"], total))
	fmt.Printf("  partially_following_standard: %8d  (%.2f%%)\n", classCount["partially"], pct(classCount["partially"], total))
	fmt.Printf("  minimally_following_standard: %8d  (%.2f%%)\n", classCount["minimally"], pct(classCount["minimally"], total))
	fmt.Printf("  not_following_standard:       %8d  (%.2f%%)\n", classCount["not"], pct(classCount["not"], total))
	fmt.Printf("  null (unclassified):          %8d  (%.2f%%)\n", classCount["null"], pct(classCount["null"], total))

	fmt.Println("\n=== Method flags (true / false / null) ===")
	for _, fn := range flagNames {
		arr := flagCount[fn]
		fmt.Printf("  %-24s  true=%-8d  false=%-8d  null=%d\n",
			fn, arr[tsTrue], arr[tsFalse], arr[tsNull])
	}

	fmt.Printf("\n=== Method combinations — is_partially_following_standard (%d tokens) ===\n", classCount["partially"])
	printPatterns(partialPatterns, classCount["partially"])

	fmt.Printf("\n=== Method combinations — ALL tokens (%d) ===\n", total)
	printPatterns(allPatterns, total)
}

func printPatterns(patterns map[methodPattern]int64, total int64) {
	type entry struct {
		pat   methodPattern
		count int64
	}
	var rows []entry
	for p, c := range patterns {
		rows = append(rows, entry{p, c})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].count > rows[j].count })

	header := "  bits  balanceOf transfer transferFrom approve allowance     count    pct"
	fmt.Println(header)
	fmt.Println("  " + strings.Repeat("-", len(header)-2))
	for _, r := range rows {
		p := r.pat
		bits := fmt.Sprintf("%s%s%s%s%s",
			boolBit(p.balanceOf), boolBit(p.transfer), boolBit(p.transferFrom),
			boolBit(p.approve), boolBit(p.allowance))
		fmt.Printf("  %5s  %-9s %-8s %-12s %-7s %-9s  %8d  %.2f%%\n",
			bits,
			boolStr(p.balanceOf), boolStr(p.transfer), boolStr(p.transferFrom),
			boolStr(p.approve), boolStr(p.allowance),
			r.count, pct(r.count, total))
	}
}

func boolBit(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func boolStr(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}
