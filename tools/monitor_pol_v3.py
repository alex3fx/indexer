#!/usr/bin/env python3
import re, socket, time, json, subprocess, os, sys

# GrayLog ingestion has no auth on this port (GELF/TCP) — no credentials to manage
# here, but the host/port are still infra-specific, so they're env-overridable.
GRAYLOG_HOST = os.environ.get("LOGS_GRAYLOG_HOST", "144.76.108.185")
GRAYLOG_PORT = int(os.environ.get("LOGS_GRAYLOG_PORT", "12201"))
APP = "indexer-pol-monitor-v3"

_RUN_DIR = os.environ.get("RUN_DIR", "/data/pol_index/full_index_run")
NODES = {
    # "proc" deliberately doesn't anchor on a space after "raw_pol_v3" — the binary
    # gets a new _suffix on every fix/redeploy (see CONTEXT.md gotcha), so matching
    # "raw_pol_v3 " literally silently breaks aliveness detection every redeploy
    # (confirmed 2026-06-23: alive=False in every heartbeat for hours after the
    # raw_pol_v3_reserve rename, even though the process was running fine).
    # Edit this dict (or set NODE_62_TO/NODE_63_FROM/NODE_63_TO env vars) to match
    # your dual-node split point — this is topology, not a credential.
    "62": {"log": os.path.join(_RUN_DIR, "run_62_v3.log"),
           "proc": f"raw_pol_v3.*--to={os.environ.get('NODE_62_TO', '31124773')}",
           "from": int(os.environ.get("NODE_62_FROM", "468640")),
           "to": int(os.environ.get("NODE_62_TO", "31124773"))},
    "63": {"log": os.path.join(_RUN_DIR, "run_63_v3.log"),
           "proc": f"raw_pol_v3.*--to={os.environ.get('NODE_63_TO', '89000000')}",
           "from": int(os.environ.get("NODE_63_FROM", "31124774")),
           "to": int(os.environ.get("NODE_63_TO", "89000000"))},
}

ACCUM_RE = re.compile(r"Accum (\d+)→(\d+): saved=(\d+) save=(\d+)ms \| ([\d.]+) blk/s avg")
ALERT_RE = re.compile(r"\[ALERT\]|\[WARNING\]|permanently missing|MissingHistoricalBlocks", re.IGNORECASE)

# Seed pos at current EOF (not 0): on every restart of this monitor itself (crash,
# manual restart, redeploy), starting from byte 0 re-scans the whole multi-GB log
# history and re-sends every old [ALERT]/[WARNING] line it finds to GrayLog as if
# new — confirmed 2026-06-23: a restart re-sent 30+ stale connection-error alerts
# from hours earlier within the first minute. Skipping to EOF means only genuinely
# new lines (written after this process starts) are ever considered.
def _initial_pos(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0

state = {n: {"pos": _initial_pos(cfg["log"]), "last_blk_s": None, "last_block": None, "alive": True}
         for n, cfg in NODES.items()}

def gelf_send(short_message, level=6, extra=None):
    payload = {
        "version": "1.1",
        "host": APP,
        "short_message": short_message,
        "level": level,
        "timestamp": time.time(),
    }
    if extra:
        for k, v in extra.items():
            payload[f"_{k}"] = v
    data = (json.dumps(payload) + "\x00").encode()
    try:
        with socket.create_connection((GRAYLOG_HOST, GRAYLOG_PORT), timeout=5) as s:
            s.sendall(data)
    except Exception as e:
        print(f"[monitor] graylog send failed: {e}", flush=True)

def is_alive(proc_name):
    r = subprocess.run(["pgrep", "-f", proc_name], capture_output=True, text=True)
    return bool(r.stdout.strip())

def tail_new(path, pos):
    if not os.path.exists(path):
        return "", pos
    size = os.path.getsize(path)
    if size < pos:
        pos = 0  # file truncated/rotated
    with open(path, "r", errors="replace") as f:
        f.seek(pos)
        data = f.read()
        new_pos = f.tell()
    return data, new_pos

def main():
    print(f"[monitor] starting, sending to {GRAYLOG_HOST}:{GRAYLOG_PORT} app={APP}", flush=True)
    cycle = 0
    while True:
        cycle += 1
        for n, cfg in NODES.items():
            st = state[n]
            data, st["pos"] = tail_new(cfg["log"], st["pos"])
            if data:
                accums = ACCUM_RE.findall(data)
                if accums:
                    last = accums[-1]
                    st["last_block"] = int(last[1])
                    st["last_blk_s"] = float(last[4])
                alerts = ALERT_RE.findall(data)
                if alerts:
                    snippet = "\n".join(l for l in data.splitlines() if ALERT_RE.search(l))[:2000]
                    gelf_send(f"[node-{n}] ALERT/WARNING detected", level=4, extra={
                        "node": n, "snippet": snippet,
                    })
                    print(f"[monitor] node-{n} ALERT/WARNING:\n{snippet}", flush=True)

            alive = is_alive(cfg["proc"])
            if st["alive"] and not alive:
                done = st["last_block"] is not None and st["last_block"] >= cfg["to"] - 50
                lvl = 6 if done else 3
                gelf_send(f"[node-{n}] process exited (last_block={st['last_block']}, target={cfg['to']})",
                          level=lvl, extra={"node": n, "last_block": st["last_block"], "expected_to": cfg["to"]})
                print(f"[monitor] node-{n} process exited, last_block={st['last_block']}", flush=True)
            st["alive"] = alive

            if cycle % 5 == 0:
                progress_pct = None
                if st["last_block"] is not None:
                    span = cfg["to"] - cfg["from"]
                    progress_pct = round(100 * (st["last_block"] - cfg["from"]) / span, 3)
                eta_h = None
                if st["last_block"] is not None and st["last_blk_s"]:
                    remaining = cfg["to"] - st["last_block"]
                    eta_h = round(remaining / st["last_blk_s"] / 3600, 1)
                gelf_send(f"[node-{n}] heartbeat block={st['last_block']} blk_s={st['last_blk_s']} alive={alive}",
                          level=6, extra={
                              "node": n, "last_block": st["last_block"], "blk_s": st["last_blk_s"],
                              "alive": alive, "progress_pct": progress_pct, "eta_hours": eta_h,
                          })
                print(f"[monitor] node-{n} heartbeat block={st['last_block']} blk_s={st['last_blk_s']} "
                      f"progress={progress_pct}% eta={eta_h}h alive={alive}", flush=True)

        time.sleep(60)

if __name__ == "__main__":
    main()
