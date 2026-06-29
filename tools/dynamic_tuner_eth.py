#!/usr/bin/env python3
"""Dynamic FETCH_WORKERS tuner + supervisor for the ETH ERC-20 indexer.

Adapted from tools/dynamic_tuner.py (Polygon indexer). Replaces the static
run_eth_07.sh / run_eth_60.sh wrapper loops: bootstrap-probes a few FETCH_WORKERS
values to find a good starting point, then continuously adjusts via AIMD driven by
the RPC node's CPU load (via Grafana/Prometheus node_load1).

Usage:
    python3 dynamic_tuner_eth.py <NODE> <RPC_HTTPS_URL> <FROM_DEFAULT> <TO_BLOCK>

    NODE          "07" or "60"
    RPC_HTTPS_URL http://100.64.0.7:8545  (or 100.64.0.60:8545)
    FROM_DEFAULT  block to start from if the log is empty (0 for node 07, 12700001 for 60)
    TO_BLOCK      12700000 for node 07; 999000000 for node 60 (tuner never stops naturally)

Required env vars:
    SCYLLA_DB_PASSWORD   Scylla password (no default — must be set)
    REDIS_PASSWORD       Redis password (no default — must be set)

Recommended env vars (AIMD works blind without them):
    GRAFANA_USER / GRAFANA_PASS    Grafana credentials
    GRAFANA_URL / GRAFANA_PROM_UID Grafana instance + Prometheus datasource UID

See TUNER_ETH.md for full launch commands and resume instructions.
"""
import os, re, sys, time, socket, json, signal, subprocess, base64
import urllib.request, urllib.parse

NODE = sys.argv[1]              # "07" or "60"
RPC_URL = sys.argv[2]           # http://100.64.0.7:8545 or .60:8545
FROM_DEFAULT = int(sys.argv[3])  # used only if log has no resume point yet
TO_BLOCK = int(sys.argv[4])
# TO_BLOCK=0 → realtime mode: binary launched without --to (discovers HEAD from WSS,
# does historical catch-up, then enters WSS realtime loop). Tuner skips the
# "reached target" stop condition and never terminates on its own.
REALTIME = (TO_BLOCK == 0)

BIN = os.environ.get("INDEXER_BIN", "/home/alexey_smolyakov/raw_erc20_v8")

_WSS_DEFAULTS = {
    "07": "ws://100.64.0.7:8546",
    "60": "ws://100.64.0.60:8546",
}
WSS_URL = os.environ.get("PRIMARY_RPC_WSS", _WSS_DEFAULTS.get(NODE, ""))

_NEIGHBOR_DEFAULTS = {
    "07": "http://100.64.0.60:8545",
    "60": "http://100.64.0.7:8545",
}
NEIGHBOR_RPC_URL = os.environ.get("NEIGHBOR_RPC_URL", _NEIGHBOR_DEFAULTS.get(NODE, ""))
RESERVE_RPC = os.environ.get("RESERVE_RPC_URL", "https://ethereum-rpc.publicnode.com")

LOG = os.environ.get("TUNER_LOG", f"/home/alexey_smolyakov/eth_index_{NODE}.log")
REDIS_DB = int(os.environ.get("REDIS_DB", {"07": 1, "60": 2}.get(NODE, 3)))
REDIS_PASS = os.environ.get("REDIS_PASSWORD")
if not REDIS_PASS:
    sys.exit("REDIS_PASSWORD must be set in the environment (no default)")

GRAYLOG_HOST = os.environ.get("LOGS_GRAYLOG_HOST", "144.76.108.185")
GRAYLOG_PORT = int(os.environ.get("LOGS_GRAYLOG_PORT", "12201"))
APP = f"indexer-eth-{NODE}-tuner"

SCYLLA_DB_HOST = os.environ.get("SCYLLA_DB_HOST", "127.0.0.1")
SCYLLA_DB_PORT = os.environ.get("SCYLLA_DB_PORT", "9042")
SCYLLA_DB_KEYSPACE = os.environ.get("SCYLLA_DB_KEYSPACE", "eth")
SCYLLA_DB_USERNAME = os.environ.get("SCYLLA_DB_USERNAME")
if not SCYLLA_DB_USERNAME:
    sys.exit("SCYLLA_DB_USERNAME must be set in the environment (no default)")
SCYLLA_DB_PASSWORD = os.environ.get("SCYLLA_DB_PASSWORD")
if not SCYLLA_DB_PASSWORD:
    sys.exit("SCYLLA_DB_PASSWORD must be set in the environment (no default)")

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.lotos-team.com")
GRAFANA_USER = os.environ.get("GRAFANA_USER")
GRAFANA_PASS = os.environ.get("GRAFANA_PASS")
GRAFANA_PROM_UID = os.environ.get("GRAFANA_PROM_UID", "PBFA97CFB590B2093")

# node .7 (157.90.65.123): 48 cores; node .60: 96 cores (confirmed via node_exporter)
_NODE_INFO = {
    "07": {"instance": os.environ.get("NODE_07_INSTANCE", "157.90.65.123:9100"), "cores": 48},
    "60": {"instance": os.environ.get("NODE_60_INSTANCE", "100.64.0.60:9100"),   "cores": 96},
}

# Load target band — same as Polygon (.85/.95 empirically good).
_LOAD_TARGETS = {
    "07": {"low": 0.85, "high": 0.95},
    "60": {"low": 0.85, "high": 0.95},
}
_load_target = _LOAD_TARGETS.get(NODE, {"low": 0.55, "high": 0.80})
LOAD_TARGET_LOW = _load_target["low"]
LOAD_TARGET_HIGH = _load_target["high"]
LOAD_INCREASE_FACTOR = 1.15
LOAD_DECREASE_FACTOR = 0.75
LOAD_CHANGE_COOLDOWN_SEC = 90

# node .7 benchmark peak: 64 workers → 204.9 blk/s; 96 → 202.9 (no gain).
# node .60 (96 cores, unknown ETH node software load) — start with wider range.
_WORKER_BOUNDS = {
    "07": {"min": 4,  "max": 96,  "probe": [16, 32, 48, 64]},
    "60": {"min": 8,  "max": 200, "probe": [32, 48, 64, 96]},
}
_bounds = _WORKER_BOUNDS.get(NODE, {"min": 8, "max": 128, "probe": [16, 32, 64, 96]})
MIN_WORKERS = _bounds["min"]
MAX_WORKERS = _bounds["max"]
PROBE_VALUES_INIT = _bounds["probe"]
PROBE_DURATION_SEC = 90
CHECK_INTERVAL_SEC = 30
ERROR_BACKOFF_FACTOR = 0.6
ERROR_DELTA_THRESHOLD = 10
RESTART_WINDOW_SEC = 300
RESTART_BACKOFF_THRESHOLD = 3

ACCUM_RE = re.compile(r"Accum (\d+)→(\d+): saved=(\d+) save=(\d+)ms")
ALERT_RE = re.compile(r"\[ALERT\] (\d+) connection errors")
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


_log_scan_state = {"pos": 0, "last_block": None, "last_errors": 0, "last_wm": None}


def _scan_log_incremental():
    if not os.path.exists(LOG):
        return _log_scan_state
    size = os.path.getsize(LOG)
    if size < _log_scan_state["pos"]:
        _log_scan_state["pos"] = 0
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
    s = _scan_log_incremental()
    return s["last_block"], s["last_errors"]


def last_watermark():
    return _scan_log_incremental()["last_wm"]


def resume_from():
    wm = last_watermark()
    if wm is not None:
        return wm
    last_block, _ = log_tail_state()
    if last_block is None:
        return FROM_DEFAULT
    return last_block + 1


def start_indexer(from_block, to_block, fetch_workers):
    env = os.environ.copy()
    env.update({
        "MODE": "production",
        "EVM_CHAIN_ID": "1",
        "PRIMARY_RPC_HTTPS": RPC_URL,
        "PRIMARY_RPC_WSS": WSS_URL,
        "BACKUP_RPC_HTTPS": NEIGHBOR_RPC_URL,
        "BACKUP_RPC_HTTPS_2": RESERVE_RPC,
        "CM_CONNECTION_URL": os.environ.get(
            "CM_CONNECTION_URL",
            f"redis://:{REDIS_PASS}@127.0.0.1:6379/{REDIS_DB}",
        ),
        "SCYLLA_DB_HOST": SCYLLA_DB_HOST,
        "SCYLLA_DB_PORT": SCYLLA_DB_PORT,
        "SCYLLA_DB_KEYSPACE": SCYLLA_DB_KEYSPACE,
        "SCYLLA_DB_USERNAME": SCYLLA_DB_USERNAME,
        "SCYLLA_DB_PASSWORD": SCYLLA_DB_PASSWORD,
        "SCYLLA_CHUNK_BUCKETS": "24",
        "SCYLLA_CHUNK_ERA": "12000",
        "FETCH_WORKERS": str(fetch_workers),
        "SAVE_EVERY": "100",
        "LOGS_GRAYLOG_HOST": GRAYLOG_HOST,
        "LOGS_GRAYLOG_PORT": str(GRAYLOG_PORT),
        "LOGS_GRAYLOG_APP": f"indexer-eth-{NODE}",
    })
    # TO_BLOCK=0 is the realtime sentinel: omit --to so the binary queries
    # current HEAD from WSS, catches up historically, then stays in WSS realtime.
    realtime = (to_block == 0)
    logf = open(LOG, "a")
    logf.write(f"\n[tuner] starting --from={from_block}{'' if realtime else f' --to={to_block}'} FETCH_WORKERS={fetch_workers}{'  [realtime]' if realtime else ''}\n")
    logf.flush()
    cmd = [BIN, f"--from={from_block}"] if realtime else [BIN, f"--from={from_block}", f"--to={to_block}"]
    proc = subprocess.Popen(cmd, env=env, stdout=logf, stderr=subprocess.STDOUT)
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
    start_block, start_errors = log_tail_state()
    if start_block is None:
        start_block = from_block - 1
    t0 = time.time()
    while time.time() - t0 < duration_sec:
        if proc_holder["proc"].poll() is not None:
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
    gelf_send(f"[node-{NODE}] eth dynamic tuner starting", level=6)
    mode_str = "realtime (no --to)" if REALTIME else f"historical to={TO_BLOCK}"
    print(f"[tuner-{NODE}] starting, log={LOG}, FROM_DEFAULT={FROM_DEFAULT}, mode={mode_str}", flush=True)

    fetch_workers = PROBE_VALUES_INIT[len(PROBE_VALUES_INIT) // 2]
    from_block = resume_from()
    if not REALTIME and from_block > TO_BLOCK:
        print(f"[tuner-{NODE}] already past target {TO_BLOCK}, nothing to do", flush=True)
        return

    proc = start_indexer(from_block, TO_BLOCK, fetch_workers)
    proc_holder = {"proc": proc}

    probe_candidates = list(PROBE_VALUES_INIT)
    results = {}
    overloaded_candidates = set()
    recent_restarts = []
    last_change_time = 0.0

    mode = "probing"
    probe_idx = 0
    last_check_block, last_check_errors = None, 0

    while True:
        cur_from = resume_from()
        if not REALTIME and cur_from > TO_BLOCK:
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
            results[candidate] = rate
            load_str = f"load1={load1:.1f} (ratio={load_ratio:.2f})" if load1 is not None else "load1=unavailable"
            print(f"[tuner-{NODE}] probe FETCH_WORKERS={candidate} -> {rate:.2f} blk/s "
                  f"(processed={processed}, conn_errors+={err_delta}, {load_str})", flush=True)
            gelf_send(f"[node-{NODE}] probe FETCH_WORKERS={candidate} -> {rate:.2f} blk/s",
                      level=6, extra={"node": NODE, "fetch_workers": candidate, "blk_s": rate,
                                       "conn_errors_delta": err_delta, "load1": load1})

            if not REALTIME and exited and resume_from() > TO_BLOCK:
                continue

            overloaded = (err_delta >= ERROR_DELTA_THRESHOLD
                          or (load_ratio is not None and load_ratio >= LOAD_TARGET_HIGH))
            if overloaded:
                reason = f"{err_delta} conn errors" if err_delta >= ERROR_DELTA_THRESHOLD else f"load_ratio={load_ratio:.2f}"
                print(f"[tuner-{NODE}] FETCH_WORKERS={candidate} caused {reason} — stopping probe escalation", flush=True)
                gelf_send(f"[node-{NODE}] FETCH_WORKERS={candidate} too aggressive ({reason})",
                          level=4, extra={"node": NODE, "fetch_workers": candidate})
                overloaded_candidates.add(candidate)
                probe_idx = len(probe_candidates)
            else:
                probe_idx += 1

            if probe_idx >= len(probe_candidates):
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

        else:  # steady state
            time.sleep(CHECK_INTERVAL_SEC)
            if proc_holder["proc"].poll() is not None:
                cur = resume_from()
                if not REALTIME and cur > TO_BLOCK:
                    continue
                now = time.time()
                recent_restarts[:] = [t for t in recent_restarts if now - t < RESTART_WINDOW_SEC]
                recent_restarts.append(now)
                print(f"[tuner-{NODE}] process exited unexpectedly "
                      f"({len(recent_restarts)} restarts in last {RESTART_WINDOW_SEC}s), restarting", flush=True)
                if len(recent_restarts) >= RESTART_BACKOFF_THRESHOLD and fetch_workers > MIN_WORKERS:
                    new_workers = max(MIN_WORKERS, int(fetch_workers * ERROR_BACKOFF_FACTOR))
                    print(f"[tuner-{NODE}] {len(recent_restarts)} restarts -> backing off "
                          f"FETCH_WORKERS {fetch_workers}->{new_workers}", flush=True)
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
            rate = ((cur_block - last_check_block) / CHECK_INTERVAL_SEC
                    if (cur_block and last_check_block) else 0.0)

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

            load1, load_ratio = grafana_load1()
            now = time.time()
            in_cooldown = (now - last_change_time) < LOAD_CHANGE_COOLDOWN_SEC
            load_str = f"load1={load1:.1f} ratio={load_ratio:.2f}" if load1 is not None else "load1=unavailable"
            print(f"[tuner-{NODE}] steady: {rate:.1f} blk/s, FETCH_WORKERS={fetch_workers}, {load_str}"
                  f"{' (cooldown)' if in_cooldown else ''}", flush=True)

            if load_ratio is None or in_cooldown:
                continue

            if load_ratio >= LOAD_TARGET_HIGH and fetch_workers > MIN_WORKERS:
                new_workers = max(MIN_WORKERS, int(fetch_workers * LOAD_DECREASE_FACTOR))
                if new_workers != fetch_workers:
                    print(f"[tuner-{NODE}] load_ratio={load_ratio:.2f} >= {LOAD_TARGET_HIGH} -> "
                          f"backing off FETCH_WORKERS {fetch_workers}->{new_workers}", flush=True)
                    gelf_send(f"[node-{NODE}] load-driven backoff FETCH_WORKERS {fetch_workers}->{new_workers}",
                              level=4, extra={"node": NODE, "old": fetch_workers, "new": new_workers,
                                               "load1": load1, "load_ratio": load_ratio})
                    stop_indexer(proc_holder["proc"])
                    fetch_workers = new_workers
                    proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                    last_check_block, last_check_errors = log_tail_state()
                    last_change_time = now
            elif load_ratio <= LOAD_TARGET_LOW and fetch_workers < MAX_WORKERS:
                new_workers = min(MAX_WORKERS, max(fetch_workers + 1, int(fetch_workers * LOAD_INCREASE_FACTOR)))
                if new_workers != fetch_workers:
                    print(f"[tuner-{NODE}] load_ratio={load_ratio:.2f} <= {LOAD_TARGET_LOW} -> "
                          f"raising FETCH_WORKERS {fetch_workers}->{new_workers}", flush=True)
                    gelf_send(f"[node-{NODE}] load-driven raise FETCH_WORKERS {fetch_workers}->{new_workers}",
                              level=6, extra={"node": NODE, "old": fetch_workers, "new": new_workers,
                                               "load1": load1, "load_ratio": load_ratio})
                    stop_indexer(proc_holder["proc"])
                    fetch_workers = new_workers
                    proc_holder["proc"] = start_indexer(resume_from(), TO_BLOCK, fetch_workers)
                    last_check_block, last_check_errors = log_tail_state()
                    last_change_time = now


if __name__ == "__main__":
    main()
