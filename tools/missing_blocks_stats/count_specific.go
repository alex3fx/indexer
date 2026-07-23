// count_specific: counts TX/LOG/ITX rows in Scylla for a specific list of block numbers.
// Reads block numbers from stdin (one per line). Used to check whether missing-BC blocks
// have their data stored in Scylla or not.
//
// +build ignore
package main
