# TODO / Known upstream bugs

## Zig 0.17-dev: SIGILL in std.crypto.ml_kem under -Doptimize=ReleaseFast

**Status:** worked around in this codebase (not blocking), root cause NOT found, not reported upstream yet.

**Symptom:** any TLS 1.3 handshake done via `std.http.Client` (i.e. any `https://` request)
crashes with `Illegal instruction` (SIGILL) when the binary is compiled with
`-Doptimize=ReleaseFast`. Confirmed via Debug build + `addr2line`: the crash site is
`Poly.decompress` in `lib/std/crypto/ml_kem.zig:1105`, triggered because Zig's TLS client
always offers `x25519_ml_kem768` (post-quantum hybrid key exchange) in the ClientHello.

**What's been tried:**
- Patched the local toolchain (`/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/lib/std/crypto/tls/Client.zig`)
  to remove `x25519_ml_kem768` from `supported_groups`/`key_share`. This **fixes `-Doptimize=ReleaseSafe`**
  (confirmed via clean rebuild) but **does NOT fix `-Doptimize=ReleaseFast`** — still SIGILLs, even
  after clearing both the local and global Zig caches (ruled out as a stale-cache artifact). This
  means there is a **second, still-unfound codegen bug** specific to aggressive optimization,
  somewhere in the crypto/TLS path (possibly still in `ml_kem.zig`'s SIMD code, possibly elsewhere
  in the patched call chain — not isolated).
- `addr2line` is unavailable against the `ReleaseFast` binary by default (`strip=true`), so the
  ReleaseFast-specific crash site has not been pinpointed.
- An isolated minimal repro (outside this binary, `/tmp/tlstest/`, not preserved) did **not**
  reproduce the crash — single HTTPS call and HTTP→HTTPS-on-one-client sequences both worked fine
  standalone. The crash only reproduces inside the real binary's actual runtime context
  (same `gpa`/`io: std.Io` as `main.zig`'s `Init`), suggesting some allocator/threading/Io-implementation
  interaction rather than a pure crypto-math bug — worth retrying a minimal repro using
  `std.process.Init`'s actual `io` (e.g. `std.Io.Threaded.init`) instead of a simplified standalone one.

**Current workaround (shipped, see `src/core/common/fetch.zig`):** any `https://` URL is routed
through a `curl` subprocess (`fetchViaCurl`) instead of `std.http.Client`, entirely bypassing Zig's
TLS stack. This is safe here because HTTPS is only ever the backup/public-RPC fallback tier
(rare, not the hot path) — subprocess overhead is irrelevant. Confirmed working under
`-Doptimize=ReleaseFast` via `zig test` directly against a real public Polygon RPC endpoint.

**Why this is still a TODO and not fully closed:**
- The actual Zig compiler bug is unfixed and unreported — anyone hitting `std.http.Client` HTTPS
  under `ReleaseFast` on this same Zig version will hit the same SIGILL.
- The local toolchain patch only fixes `ReleaseSafe`; it's incomplete and lives outside version
  control (`/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4`, not this repo) — it will
  not survive a toolchain reinstall/upgrade and is not portable to any other machine/CI.
- If a future Zig point release moves past this dev snapshot, re-check whether the bug still
  reproduces before reverting the curl workaround.

**Suggested next steps (not done, low priority since workaround is sufficient for now):**
1. Build a `ReleaseFast` binary with `strip=false` (override in `build.zig`) to get `addr2line`
   working, reproduce the SIGILL, and find the actual crash site.
2. Once pinpointed, file an upstream Zig issue with a minimal repro that uses
   `std.Io.Threaded.init` (matching this codebase's real runtime) rather than a simplified
   allocator/Io setup, since the simplified repro attempt did not reproduce the bug.
3. Decide whether to keep the curl-subprocess workaround permanently (probably yes — it's simpler
   and removes a fragile out-of-repo toolchain dependency) even after the Zig bug is eventually fixed.
