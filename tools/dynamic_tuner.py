#!/usr/bin/env python3
"""Dynamic FETCH_WORKERS tuner + supervisor for the Polygon indexer.

Replaces the static bash loop wrapper: bootstrap-probes a few FETCH_WORKERS values to
find a reasonable starting point, then continuously adjusts via an AIMD-style control
loop driven by the RPC node's actual CPU load (via Grafana/Prometheus node_load1) —
not just connection errors. Connection-error and crash-loop backoff remain as fast
safety nets for failure modes load1 doesn't see immediately (e.g. a sudden RPC outage).

Why load-based control: the original tuner only reacted to connection errors, so it
would happily climb FETCH_WORKERS back up to levels that pinned the RPC node at 100%
CPU+iowait (load1 == core count) with ZERO connection errors — slow degraded responses
aren't "errors", they're just slow, so the old logic was blind to the real constraint.
A static worker cap either leaves throughput on the table (cap too low) or still
overloads the node (cap too high, since the safe ceiling drifts with chain density
and node state). Closed-loop control finds the actual sweet spot instead.

Resume logic: parse the last "[watermark] N" line from this node's own log file —
that is the true contiguous-safe frontier (NOT the shared Redis checkpoint, which
collides between the two dual-node processes, and NOT the last "Accum X->Y" line,
which can be ahead of the watermark under concurrency).
"""
import os, re, sys, time, socket, json, signal, subprocess, base64
import urllib.request, urllib.parse

NODE = sys.argv[1]              # "62" or "63"
RPC_URL = sys.argv[2]           # http://100.64.0.6X:8545
FROM_DEFAULT = int(sys.argv[3])  # used only if log has no resume point yet
TO_BLOCK = int(sys.argv[4])

# ─── All infra topology + credentials below come from env, with this deployment's
# current values as defaults (NOT secrets — IPs/paths/hostnames) so the script keeps
# working out of the box on this infra. The one exception is SCYLLA_DB_PASSWORD,
# which has NO default — it must be set in the environment, never hardcoded/committed.
# See docs/HOWTOSTART.md for the full list of required/optional env vars. ──────────
BIN = os.environ.get("INDEXER_BIN", "/data/pol_index/raw_pol_v3_reserve")
# Second retry tier (primary -> neighbor -> backup -> explicit skip+record, see
# pipeline.zig worker()) — the OTHER dual-node split partner's RPC. Both are
# plain HTTP, so this doesn't hit the HTTPS/ML-KEM SIGILL (see TODO.md).
_NEIGHBOR_DEFAULTS = {"62": "http://100.64.0.63:8545", "63": "http://100.64.0.62:8545"}
NEIGHBOR_RPC_URL = os.environ.get("NEIGHBOR_RPC_URL", _NEIGHBOR_DEFAULTS.get(NODE, ""))
LOG = os.environ.get("TUNER_LOG", f"/data/pol_index/full_index_run/run_{NODE}_v3.log")
# Separate Redis logical DB per node — both processes used to share db=0's
# LATEST_PROCESSED_BLOCK_NUMBER checkpoint key, overwriting each other's
# progress. Historical resume doesn't actually read it back (we resume from
# this node's own log via resume_from()), so it was inert, not data-lossy —
# but it's a landmine for anything that does read it (monitoring, future
# realtime cutover). REDIS_DB defaults to 0 for unknown node ids.
REDIS_DB = int(os.environ.get("REDIS_DB", {"62": 0, "63": 1}.get(NODE, 0)))
RESERVE_RPC = os.environ.get("RESERVE_RPC_URL", "https://polygon-bor-rpc.publicnode.com")

GRAYLOG_HOST = os.environ.get("LOGS_GRAYLOG_HOST", "144.76.108.185")
GRAYLOG_PORT = int(os.environ.get("LOGS_GRAYLOG_PORT", "12201"))
APP = f"indexer-pol-{NODE}-tuner"

# Scylla credentials for the spawned indexer process — both REQUIRED, no defaults.
# Host/port/keyspace have safe (non-secret) defaults.
SCYLLA_DB_HOST = os.environ.get("SCYLLA_DB_HOST", "127.0.0.1")
SCYLLA_DB_PORT = os.environ.get("SCYLLA_DB_PORT", "9042")
SCYLLA_DB_KEYSPACE = os.environ.get("SCYLLA_DB_KEYSPACE", "pol")
SCYLLA_DB_USERNAME = os.environ.get("SCYLLA_DB_USERNAME")
if not SCYLLA_DB_USERNAME:
    sys.exit("SCYLLA_DB_USERNAME must be set in the environment (no default)")
SCYLLA_DB_PASSWORD = os.environ.get("SCYLLA_DB_PASSWORD")
if not SCYLLA_DB_PASSWORD:
    sys.exit("SCYLLA_DB_PASSWORD must be set in the environment (no default)")

# ─── Grafana/Prometheus — the real congestion signal (see module docstring) ────────
# Credentials come from env, never hardcoded here (GRAFANA_USER/GRAFANA_PASS). If
# unset, grafana_load1() just returns (None, None) and the tuner falls back to the
# old error-only behavior — degrades safely, doesn't crash.
GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.lotos-team.com")
GRAFANA_USER = os.environ.get("GRAFANA_USER")
GRAFANA_PASS = os.environ.get("GRAFANA_PASS")
GRAFANA_PROM_UID = os.environ.get("GRAFANA_PROM_UID", "PBFA97CFB590B2093")
_NODE_INFO = {
    "62": {"instance": os.environ.get("NODE_62_INSTANCE", "100.64.0.62:9100"), "cores": 20},
    "63": {"instance": os.environ.get("NODE_63_INSTANCE", "100.64.0.63:9100"), "cores": 96},
}
# Disk-busy signal — present for visibility but no longer gates AIMD decisions for .63.
# The NVMe's busy% reads ~99-100% essentially independent of actual indexer load
# (it reflects "queue non-empty" on a high-IOPS drive, not "can't accept more work").
# A manual FETCH_WORKERS sweep confirmed throughput scales roughly linearly well
# past the point where disk_busy was already pegged at 100% — the signal was a
# false ceiling. Left in place (still logged) but `disk_high`/`disk_low_or_unknown`
# below are now hardcoded inert for .63.
_DISK_INFO = {
    "63": {"instance": _NODE_INFO["63"]["instance"], "device": os.environ.get("NODE_63_DISK_DEVICE", "nvme2n1")},
}
DISK_TARGET_LOW = 0.50
DISK_TARGET_HIGH = 0.80
# Per-node load target band. Both nodes use the same band (0.85/0.95): empirical
# sweep on .63 showed sustained load_ratio~0.96 holds peak throughput without collapse,
# and the empirical peak was ~55-60 workers (load1~85-95 on 96 cores).
_LOAD_TARGETS = {
    "62": {"low": 0.85, "high": 0.95},
    "63": {"low": 0.85, "high": 0.95},
}
_load_target = _LOAD_TARGETS.get(NODE, {"low": 0.55, "high": 0.80})
LOAD_TARGET_LOW = _load_target["low"]
LOAD_TARGET_HIGH = _load_target["high"]
LOAD_INCREASE_FACTOR = 1.15
LOAD_DECREASE_FACTOR = 0.75
LOAD_CHANGE_COOLDOWN_SEC = 90  # don't act again until load has had time to reflect the
                                # last change — load1 lags FETCH_WORKERS changes by ~30-60s
                                # in practice (queue drain / cache effects), reacting faster
                                # just oscillates on stale data.

# Per-node bounds — wide enough that the AIMD loop above has real room to find the
# sweet spot itself — wide enough that load1 feedback, not the bounds, is the
# binding constraint under normal operation.
_WORKER_BOUNDS = {
    # .62: floor kept low so AIMD has room to back off when old-block trace_block
    # requests are expensive — at higher concurrency load_ratio can exceed 1.0 quickly.
    "62": {"min": 3, "max": 150, "probe": [5, 10, 20, 40]},
    # .63: probe re-centered on empirical peak (~55-60 workers → ~290-300 blk/s);
    # max raised to 260 because AIMD was pegged at 150 with load_ratio well below
    # LOAD_TARGET_HIGH — the old cap was the binding constraint, not node load.
    "63": {"min": 30, "max": 260, "probe": [30, 45, 60, 80]},
}
_bounds = _WORKER_BOUNDS.get(NODE, {"min": 50, "max": 500, "probe": [100, 200, 300, 400]})
MIN_WORKERS = _bounds["min"]
MAX_WORKERS = _bounds["max"]
PROBE_VALUES_INIT = _bounds["probe"]
PROBE_DURATION_SEC = 90      # how long to run each bootstrap-probe candidate
CHECK_INTERVAL_SEC = 30      # how often to poll log + load1 during steady state
ERROR_BACKOFF_FACTOR = 0.6
ERROR_DELTA_THRESHOLD = 10   # conn_errors increase within one check window that triggers backoff
RESTART_WINDOW_SEC = 300     # window for counting crash-restarts (catches Scylla-side CqlError too,
                             # not just RPC conn errors — any instability that isn't self-resolving)
RESTART_BACKOFF_THRESHOLD = 3  # this many restarts within the window triggers a FETCH_WORKERS cut

ACCUM_RE = re.compile(r"Accum (\d+)→(\d+): saved=(\d+) save=(\d+)ms")
ALERT_RE = re.compile(r"\[ALERT\] (\d+) connection errors")
# [watermark] N — the contiguous-saved-frontier resume point (see pipeline.zig
# Watermark struct). NOT the same as Accum's per-batch max: a
# batch can report Accum X->Y while a slower, still-in-flight worker on a block
# < Y hasn't saved yet — resuming from Y would skip it forever on a crash/kill.
# The watermark line only ever reports a value once everything below it is
# durably saved, so it's always safe to resume from directly (no +1 needed —
# it already IS "the next block that still needs (re)fetching").
WATERMARK_RE = re.compile(r"\[watermark\] (\d+)")

current_proc = None


def gelf_send(short_message, level=6, extra=None):
    payload = {"version": "1.1", "host": APP, "short_message": short_message,
               "level": level, "timestamp": time.time()}
    if extra:
        for k, v in extra.items():
            payload[f"_{k}"] = v
    data = (json.dumps(payload) + "\x00").encode()
    try:
        with socket.create_connection((GRAYLOG_HOST, GRAYLOG_PORT), timeout=5) as s:
            s.sendall(data)
    except Exception as e:
        print(f"[tuner-{NODE}] graylog send failed: {e}", flush=True)


def grafana_load1():
    """Returns (load1, load1/cores) for this node's Grafana instance, or (None, None)
    on any failure (network, auth, missing series) — caller must treat that as
    "no signal this cycle", not as "load is zero"."""
    info = _NODE_INFO.get(NODE)
    if not info or not GRAFANA_USER or not GRAFANA_PASS:
        return None, None
    query = f'node_load1{{instance="{info["instance"]}"}}'
    url = (f"{GRAFANA_URL}/api/datasources/proxy/uid/{GRAFANA_PROM_UID}/api/v1/query"
           f"?query={urllib.parse.quote(query)}")
    req = urllib.request.Request(url)
    creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
    req.add_header("Authorization", f"Basic {creds}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        result = data["data"]["result"]
        if not result:
            return None, None
        load1 = float(result[0]["value"][1])
        return load1, load1 / info["cores"]
    except Exception as e:
        print(f"[tuner-{NODE}] grafana query failed: {e}", flush=True)
        return None, None


def grafana_disk_busy():
    """Returns disk busy ratio (0..1+) for this node's configured bottleneck device,
    or None if this node has no device configured / on any query failure. See
    _DISK_INFO comment for why this exists — load1 alone missed .63's real
    constraint (disk I/O), which is why this is a separate, optional signal."""
    info = _DISK_INFO.get(NODE)
    if not info or not GRAFANA_USER or not GRAFANA_PASS:
        return None
    query = f'rate(node_disk_io_time_seconds_total{{instance="{info["instance"]}",device="{info["device"]}"}}[1m])'
    url = (f"{GRAFANA_URL}/api/datasources/proxy/uid/{GRAFANA_PROM_UID}/api/v1/query"
           f"?query={urllib.parse.quote(query)}")
    req = urllib.request.Request(url)
    creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
    req.add_header("Authorization", f"Basic {creds}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        result = data["data"]["result"]
        if not result:
            return None
        return float(result[0]["value"][1])
    except Exception as e:
        print(f"[tuner-{NODE}] grafana disk query failed: {e}", flush=True)
        return None


# log_tail_state()/last_watermark() are called ~10x per AIMD cycle (every probe
# measurement and every steady-state check). Both used to re-scan the ENTIRE log
# file from byte 0 on every single call — fine when the log was small, but after
# days of continuous indexing it grows to tens of millions of lines, and re-reading
# the whole thing on every call becomes a real, growing source of latency in the
# tuner's own decision loop (a single probe measurement can stall past its nominal
# window). Same anti-pattern already fixed in monitor_pol_v3.py — fixed here the
# same way: keep
# a persistent byte-offset cursor and only scan newly-appended bytes since the last
# call, caching the last-seen block/errors/watermark across calls. Unlike
# monitor_pol_v3.py (which deliberately seeds pos at EOF to skip old history), this
# cursor starts at 0 so the very first call after a (re)start still does one full
# scan — that's unavoidable since resume_from() needs the true last watermark from
# the whole log — but every call after that is O(new bytes) instead of O(file size).
_log_scan_state = {"pos": 0, "last_block": None, "last_errors": 0, "last_wm": None}


def _scan_log_incremental():
    if not os.path.exists(LOG):
        return _log_scan_state
    size = os.path.getsize(LOG)
    if size < _log_scan_state["pos"]:
        _log_scan_state["pos"] = 0  # log truncated/rotated underneath us
    with open(LOG, "r", errors="replace") as f:
        f.seek(_log_scan_state["pos"])
        for line in f:
            m = ACCUM_RE.search(line)
            if m:
                _log_scan_state["last_block"] = int(m.group(2))
            m2 = ALERT_RE.search(line)
            if m2:
                _log_scan_state["last_errors"] = int(m2.group(1))
            m3 = WATERMARK_RE.search(line)
            if m3:
                _log_scan_state["last_wm"] = int(m3.group(1))
        _log_scan_state["pos"] = f.tell()
    return _log_scan_state


def log_tail_state():
    """Returns (last_block, last_conn_errors) seen anywhere in the log so far."""
    s = _scan_log_incremental()
    return s["last_block"], s["last_errors"]


def last_watermark():
    """Returns the most recent crash-safe resume point from "[watermark] N" lines,
    or None if the log has none yet (very start of a run, or pre-watermark-fix log)."""
    return _scan_log_incremental()["last_wm"]


def resume_from():
    wm = last_watermark()
    if wm is not None:
        return wm
    # Fall back to the old Accum-based resume point — only relevant before the
    # first watermark line has been printed (right after a fresh start) or for a
    # pre-watermark-fix log file from before this binary was deployed.
    last_block, _ = log_tail_state()
    if last_block is None:
        return FROM_DEFAULT
    return last_block + 1


def start_indexer(from_block, to_block, fetch_workers):
    env = os.environ.copy()
    env.update({
        "MODE": "production",
        "EVM_CHAIN_ID": "137",
        "CM_CONNECTION_URL": os.environ.get("CM_CONNECTION_URL", f"redis://127.0.0.1:6379/{REDIS_DB}"),
        "SCYLLA_DB_HOST": SCYLLA_DB_HOST,
        "SCYLLA_DB_PORT": SCYLLA_DB_PORT,
        "SCYLLA_DB_KEYSPACE": SCYLLA_DB_KEYSPACE,
        "SCYLLA_DB_USERNAME": SCYLLA_DB_USERNAME,
        "SCYLLA_DB_PASSWORD": SCYLLA_DB_PASSWORD,
        "SCYLLA_CHUNK_BUCKETS": "64",
        "SCYLLA_CHUNK_ERA": "32000",
        "WS_DELAY_MS": "0",
        "FETCH_WORKERS": str(fetch_workers),
        "ACCUM_TXS_LANES": "21",
        "ACCUM_LOG_LANES": "11",
        "ACCUM_ITX_LANES": "8",
        "RPC_URL": RPC_URL,
        "RESERVE_RPC_URL": RESERVE_RPC,
        "NEIGHBOR_RPC_URL": NEIGHBOR_RPC_URL,
        "LOGS_GRAYLOG_HOST": GRAYLOG_HOST,
        "LOGS_GRAYLOG_PORT": str(GRAYLOG_PORT),
        "LOGS_GRAYLOG_APP": f"indexer-pol-{NODE}-v3",
    })
    logf = open(LOG, "a")
    logf.write(f"\n[tuner] starting --from={from_block} --to={to_block} FETCH_WORKERS={fetch_workers}\n")
    logf.flush()
    proc = subprocess.Popen(
        [BIN, f"--from={from_block}", f"--to={to_block}"],
        env=env, stdout=logf, stderr=subprocess.STDOUT,
    )
    logf.close()
    return proc


def stop_indexer(proc):
    if proc is None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass


def measure_window(proc_holder, from_block, to_block, fetch_workers, duration_sec):
    """(Re)start the indexer at fetch_workers if not already running at that value,
    let it run for duration_sec, return (blocks_processed, conn_error_delta, still_to)."""
    start_block, start_errors = log_tail_state()
    if start_block is None:
        start_block = from_block - 1
    t0 = time.time()
    while time.time() - t0 < duration_sec:
        if proc_holder["proc"].poll() is not None:
            # process exited (crash or reached --to) — caller decides what to do
            break
        time.sleep(2)
    end_block, end_errors = log_tail_state()
    if end_block is None:
        end_block = start_block
    processed = max(0, end_block - start_block)
    err_delta = max(0, end_errors - start_errors)
    exited = proc_holder["proc"].poll() is not None
    return processed, err_delta, exited, end_block


def main():
    global current_proc
    gelf_send(f"[node-{NODE}] dynamic tuner starting", level=6)
    print(f"[tuner-{NODE}] starting, log={LOG}", flush=True)

    fetch_workers = PROBE_VALUES_INIT[len(PROBE_VALUES_INIT) // 2]
    from_block = resume_from()
    if from_block > TO_BLOCK:
        print(f"[tuner-{NODE}] already past target {TO_BLOCK}, nothing to do", flush=True)
        return

    proc = start_indexer(from_block, TO_BLOCK, fetch_workers)
    proc_holder = {"proc": proc}

    probe_candidates = list(PROBE_VALUES_INIT)
    results = {}
    overloaded_candidates = set()
    recent_restarts = []
    last_change_time = 0.0  # cooldown gate for load-based AIMD adjustments

    mode = "probing"
    probe_idx = 0

    while True:
        cur_from = resume_from()
        if cur_from > TO_BLOCK:
            print(f"[tuner-{NODE}] reached target {TO_BLOCK}, stopping", flush=True)
            gelf_send(f"[node-{NODE}] reached target block {TO_BLOCK}, tuner done", level=6)
            stop_indexer(proc_holder["proc"])
            break

        if mode == "probing":
            candidate = probe_candidates[probe_idx]
            if proc_holder["proc"].poll() is not None or fetch_workers != candidate:
                stop_indexer(proc_holder["proc"])
                fetch_workers = candidate
                proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                print(f"[tuner-{NODE}] probing FETCH_WORKERS={fetch_workers}", flush=True)

            processed, err_delta, exited, _ = measure_window(
                proc_holder, cur_from, TO_BLOCK, fetch_workers, PROBE_DURATION_SEC)
            rate = processed / PROBE_DURATION_SEC
            load1, load_ratio = grafana_load1()
            disk_busy = grafana_disk_busy()
            results[candidate] = rate
            load_str = f"load1={load1:.1f} (ratio={load_ratio:.2f})" if load1 is not None else "load1=unavailable"
            disk_str = f" disk_busy={disk_busy:.2f}" if disk_busy is not None else ""
            print(f"[tuner-{NODE}] probe FETCH_WORKERS={candidate} -> {rate:.2f} blk/s "
                  f"(processed={processed}, conn_errors+={err_delta}, {load_str}{disk_str})", flush=True)
            gelf_send(f"[node-{NODE}] probe FETCH_WORKERS={candidate} -> {rate:.2f} blk/s",
                      level=6, extra={"node": NODE, "fetch_workers": candidate, "blk_s": rate,
                                       "conn_errors_delta": err_delta, "load1": load1, "disk_busy": disk_busy})

            if exited and resume_from() > TO_BLOCK:
                continue  # loop will detect completion at top

            # Heavy error rate OR load1 overloaded during probe — drop this candidate
            # and skip remaining higher ones. disk_busy deliberately excluded here
            # (false-overload signal for high-IOPS NVMe, see _DISK_INFO comment) —
            # still measured/logged above for visibility only.
            overloaded = (err_delta >= ERROR_DELTA_THRESHOLD
                          or (load_ratio is not None and load_ratio >= LOAD_TARGET_HIGH))
            if overloaded:
                if err_delta >= ERROR_DELTA_THRESHOLD:
                    reason = f"{err_delta} conn errors"
                else:
                    reason = f"load_ratio={load_ratio:.2f}"
                print(f"[tuner-{NODE}] FETCH_WORKERS={candidate} caused {reason} "
                      f"during probe — node is struggling, stopping probe escalation", flush=True)
                gelf_send(f"[node-{NODE}] FETCH_WORKERS={candidate} too aggressive ({reason})",
                          level=4, extra={"node": NODE, "fetch_workers": candidate})
                overloaded_candidates.add(candidate)
                probe_idx = len(probe_candidates)  # force end of probing
            else:
                probe_idx += 1

            if probe_idx >= len(probe_candidates):
                # Never crown an overloaded candidate winner just because its 90s
                # snapshot had the highest raw blk/s — that's exactly the hysteresis
                # trap seen in production (a candidate can look great for 90s before
                # iowait/cache-thrashing catches up). Prefer the fastest candidate
                # that did NOT trip the overload flag; only fall back to the
                # overloaded one if literally everything tested was overloaded
                # (steady-state load-AIMD will back off from there immediately).
                safe_results = {k: v for k, v in results.items() if k not in overloaded_candidates}
                best = max(safe_results, key=safe_results.get) if safe_results else max(results, key=results.get)
                print(f"[tuner-{NODE}] probe round done: {results} (overloaded={sorted(overloaded_candidates)}) -> winner={best}", flush=True)
                gelf_send(f"[node-{NODE}] probe round done, winner FETCH_WORKERS={best}",
                          level=6, extra={"node": NODE, "results": results, "winner": best})
                if fetch_workers != best:
                    stop_indexer(proc_holder["proc"])
                    fetch_workers = best
                    proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                mode = "steady"
                last_check_block, last_check_errors = log_tail_state()
                last_change_time = time.time()
                results = {}
                overloaded_candidates = set()
                probe_idx = 0

        else:  # steady state — continuous load-aware AIMD, not a fixed re-probe schedule
            time.sleep(CHECK_INTERVAL_SEC)
            if proc_holder["proc"].poll() is not None:
                cur = resume_from()
                if cur > TO_BLOCK:
                    continue
                now = time.time()
                recent_restarts[:] = [t for t in recent_restarts if now - t < RESTART_WINDOW_SEC]
                recent_restarts.append(now)
                print(f"[tuner-{NODE}] process exited unexpectedly during steady state "
                      f"({len(recent_restarts)} restarts in last {RESTART_WINDOW_SEC}s), restarting", flush=True)
                if len(recent_restarts) >= RESTART_BACKOFF_THRESHOLD and fetch_workers > MIN_WORKERS:
                    new_workers = max(MIN_WORKERS, int(fetch_workers * ERROR_BACKOFF_FACTOR))
                    print(f"[tuner-{NODE}] {len(recent_restarts)} restarts in {RESTART_WINDOW_SEC}s "
                          f"(likely Scylla-side CqlError, not RPC) -> backing off FETCH_WORKERS "
                          f"{fetch_workers}->{new_workers}", flush=True)
                    gelf_send(f"[node-{NODE}] backing off FETCH_WORKERS {fetch_workers}->{new_workers} "
                              f"({len(recent_restarts)} crash-restarts in {RESTART_WINDOW_SEC}s)",
                              level=4, extra={"node": NODE, "old": fetch_workers, "new": new_workers})
                    fetch_workers = new_workers
                    recent_restarts.clear()
                    last_change_time = now
                proc_holder["proc"] = start_indexer(cur, TO_BLOCK, fetch_workers)
                last_check_block, last_check_errors = log_tail_state()
                continue

            cur_block, cur_errors = log_tail_state()
            err_delta = (cur_errors - last_check_errors) if cur_errors is not None else 0
            rate = ((cur_block - last_check_block) / CHECK_INTERVAL_SEC) if (cur_block and last_check_block) else 0.0

            if err_delta >= ERROR_DELTA_THRESHOLD:
                new_workers = max(MIN_WORKERS, int(fetch_workers * ERROR_BACKOFF_FACTOR))
                print(f"[tuner-{NODE}] conn_errors +{err_delta} in {CHECK_INTERVAL_SEC}s at "
                      f"FETCH_WORKERS={fetch_workers} -> backing off to {new_workers}", flush=True)
                gelf_send(f"[node-{NODE}] backing off FETCH_WORKERS {fetch_workers}->{new_workers} "
                          f"({err_delta} conn errors in {CHECK_INTERVAL_SEC}s)",
                          level=4, extra={"node": NODE, "old": fetch_workers, "new": new_workers})
                stop_indexer(proc_holder["proc"])
                fetch_workers = new_workers
                proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                last_check_block, last_check_errors = log_tail_state()
                last_change_time = time.time()
                continue

            last_check_block, last_check_errors = cur_block, cur_errors

            # Load-driven AIMD step — the actual fix for "too few never reaches max,
            # too many overloads the node": keep nudging toward the load band instead
            # of sitting at a fixed cap or a stale probe-round winner. disk_busy is
            # measured/logged for visibility only — NOT used to gate raise/backoff
            # decisions (false-overload signal for high-IOPS NVMe; busy% reads ~100%
            # near-constantly regardless of actual headroom, see _DISK_INFO comment).
            # load_ratio alone drives both raise and backoff.
            load1, load_ratio = grafana_load1()
            disk_busy = grafana_disk_busy()
            now = time.time()
            in_cooldown = (now - last_change_time) < LOAD_CHANGE_COOLDOWN_SEC
            load_str = f"load1={load1:.1f} ratio={load_ratio:.2f}" if load1 is not None else "load1=unavailable"
            disk_str = f" disk_busy={disk_busy:.2f}" if disk_busy is not None else ""
            print(f"[tuner-{NODE}] steady: {rate:.1f} blk/s, FETCH_WORKERS={fetch_workers}, {load_str}{disk_str}"
                  f"{' (cooldown)' if in_cooldown else ''}", flush=True)

            if load_ratio is None or in_cooldown:
                continue

            if load_ratio >= LOAD_TARGET_HIGH and fetch_workers > MIN_WORKERS:
                new_workers = max(MIN_WORKERS, int(fetch_workers * LOAD_DECREASE_FACTOR))
                if new_workers != fetch_workers:
                    trigger = f"load_ratio={load_ratio:.2f} >= {LOAD_TARGET_HIGH}"
                    print(f"[tuner-{NODE}] {trigger} -> "
                          f"backing off FETCH_WORKERS {fetch_workers}->{new_workers}", flush=True)
                    gelf_send(f"[node-{NODE}] load-driven backoff FETCH_WORKERS {fetch_workers}->{new_workers}",
                              level=4, extra={"node": NODE, "old": fetch_workers, "new": new_workers,
                                               "load1": load1, "load_ratio": load_ratio, "disk_busy": disk_busy})
                    stop_indexer(proc_holder["proc"])
                    fetch_workers = new_workers
                    proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                    last_check_block, last_check_errors = log_tail_state()
                    last_change_time = now
            elif load_ratio <= LOAD_TARGET_LOW and fetch_workers < MAX_WORKERS:
                new_workers = min(MAX_WORKERS, max(fetch_workers + 1, int(fetch_workers * LOAD_INCREASE_FACTOR)))
                if new_workers != fetch_workers:
                    print(f"[tuner-{NODE}] load_ratio={load_ratio:.2f} <= {LOAD_TARGET_LOW}{disk_str} -> "
                          f"raising FETCH_WORKERS {fetch_workers}->{new_workers}", flush=True)
                    gelf_send(f"[node-{NODE}] load-driven raise FETCH_WORKERS {fetch_workers}->{new_workers}",
                              level=6, extra={"node": NODE, "old": fetch_workers, "new": new_workers,
                                               "load1": load1, "load_ratio": load_ratio, "disk_busy": disk_busy})
                    stop_indexer(proc_holder["proc"])
                    fetch_workers = new_workers
                    proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                    last_check_block, last_check_errors = log_tail_state()
                    last_change_time = now


if __name__ == "__main__":
    main()
