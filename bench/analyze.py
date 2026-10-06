#!/usr/bin/env python3
"""Summarise benchmark jobs copied back from the cluster.

    bench/analyze.py --raw context/benchmarks/raw --throughput 103700 --recovery 103701 103702 103703
    bench/analyze.py --meta 103710 --snapshot 103709

Each job directory (from bench/slurm/*.sbatch) has raw/<tag>.csv files with
one line per client: kind,id,host,bytes,start_ns,end_ns,extra. A run's
aggregate throughput is total bytes / (last end - first start), so a slow
client lowers the result rather than being averaged away. MB = 10^6 bytes.

Writes summary CSVs, plots and the per-client raw CSVs to --out, and prints
markdown tables.
"""
import argparse
import csv
import glob
import os
import re
import shutil
import statistics as st

MB = 1e6


def load(path):
    with open(path) as f:
        return [r for r in csv.reader(f) if r and r[0] not in ("",)]


def aggregate(rows):
    b = sum(int(r[3]) for r in rows)
    s = min(int(r[4]) for r in rows)
    e = max(int(r[5]) for r in rows)
    return b / MB / ((e - s) / 1e9), b, (e - s) / 1e9


def copy_raw(job_dir, out, job):
    dst = os.path.join(out, "raw", job)
    os.makedirs(dst, exist_ok=True)
    for f in glob.glob(os.path.join(job_dir, "raw", "*.csv")):
        shutil.copy(f, dst)


def throughput(raw, jobs, out):
    runs = []  # (job, kind, mode, n, run, mbps, bytes, secs)
    net = {}
    for job in jobs:
        d = os.path.join(raw, job)
        copy_raw(d, out, job)
        env = open(os.path.join(d, "env.txt")).read()
        m = re.search(r"OP_W=(\S+)", env)
        op_w = m.group(1) if m else "1M"
        m = re.search(r"APPEND_RECORD=(\S+)", env)
        op_a = m.group(1) if m else "1M"
        for f in sorted(glob.glob(os.path.join(d, "raw", "*.csv"))):
            name = os.path.basename(f)[:-4]
            m = re.fullmatch(r"(read|write|append)-(avoid|local)-n(\d+)-r(\d+)", name)
            rows = load(f)
            if m and rows:
                kind, mode, n, run = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
                if len(rows) != n:
                    print(f"skip {job}/{name}: {len(rows)} of {n} clients finished")
                    continue
                op = {"read": "4M", "write": op_w, "append": op_a}[kind]
                runs.append((job, kind, mode, op, n, run, *aggregate(rows)))
            elif name in ("net-pair", "net-ring") and rows:
                per = [int(r[3]) / MB / ((int(r[5]) - int(r[4])) / 1e9) for r in rows]
                net.setdefault(name, []).append(per)
    with open(os.path.join(out, "throughput_runs.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["job", "kind", "mode", "op", "clients", "run", "MBps", "bytes", "seconds"])
        for r in runs:
            w.writerow([*r[:6], f"{r[6]:.1f}", r[7], f"{r[8]:.2f}"])

    # Per-NIC rate from the ring test: every node sends and receives at once.
    nic = st.median([x for per in net.get("net-ring", []) for x in per]) if net.get("net-ring") else None
    pair = st.median([x for per in net.get("net-pair", []) for x in per]) if net.get("net-pair") else None

    groups = {}
    for r in runs:
        groups.setdefault((r[1], r[2], r[3], r[4]), []).append(r[6])
    summary = []  # (kind, mode, op, n, runs, median, min, max, limit)
    for (kind, mode, op, n), v in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], -opbytes(kv[0][2]), kv[0][3])):
        summary.append((kind, mode, op, n, len(v), st.median(v), min(v), max(v), limit(kind, mode, n, nic)))
    with open(os.path.join(out, "throughput_summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["kind", "mode", "op", "clients", "runs", "median_MBps", "min_MBps", "max_MBps", "network_limit_MBps"])
        for s in summary:
            w.writerow([*s[:5], f"{s[5]:.1f}", f"{s[6]:.1f}", f"{s[7]:.1f}", f"{s[8]:.1f}" if s[8] else ""])

    print(f"\nNetwork: single TCP stream {fmt(pair)} MB/s; per-NIC with all nodes sending+receiving {fmt(nic)} MB/s\n")
    print("| op | client mode | op size | clients | runs | median MB/s | min–max | network limit | % of limit |")
    print("|---|---|---|---:|---:|---:|---|---:|---:|")
    for kind, mode, op, n, k, med, lo, hi, lim in summary:
        pct = f"{100 * med / lim:.0f}%" if lim else ""
        print(f"| {kind} | {mode} | {op} | {n} | {k} | {med:.1f} | {lo:.1f}–{hi:.1f} | {fmt(lim)} | {pct} |")
    plot(summary, nic, out)


# Idealised network limit for the 6-node layout: clients on min(N, 6) nodes,
# chunkservers on 5. "avoid" mode puts every replica transfer on the wire.
CS_NODES, ALL_NODES = 5, 6


def limit(kind, mode, n, nic):
    if not nic or mode != "avoid":
        return None
    client_nodes = min(n, ALL_NODES)
    if kind == "read":  # each byte leaves one chunkserver NIC, enters one client NIC
        return nic * min(client_nodes, CS_NODES)
    if kind == "append":  # every client appends to the same chunk: its 3 replicas each receive every byte
        return min(nic * client_nodes, nic)
    # each byte enters 3 chunkserver NICs (client -> r1 -> r2 -> r3)
    return min(nic * client_nodes, nic * CS_NODES / 3)


def opbytes(op):
    return int(op[:-1]) << {"K": 10, "M": 20, "G": 30}[op[-1]]


def fmt(x):
    return f"{x:.1f}" if x else "–"


def plot(summary, nic, out):
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not available; skipping plots")
        return
    kinds = [k for k in ("read", "write", "append") if any(s[0] == k for s in summary)]
    fig, axes = plt.subplots(1, len(kinds), figsize=(5.5 * len(kinds), 4.2), squeeze=False)
    for ax, kind in zip(axes[0], kinds):
        series = sorted({(s[1], s[2]) for s in summary if s[0] == kind}, key=lambda x: (x[0], -opbytes(x[1])))
        for mode, op in series:
            style = ("o-" if mode == "avoid" else "s--") if op in ("4M", "8M") else "^:"
            label = ("network-only" if mode == "avoid" else "nearest replica (may be local)") + f", {op[:-1]} MB ops"
            pts = sorted((s[3], s[5], s[6], s[7]) for s in summary if s[0] == kind and s[1] == mode and s[2] == op)
            ax.errorbar([p[0] for p in pts], [p[1] for p in pts], yerr=[[p[1] - p[2] for p in pts], [p[3] - p[1] for p in pts]], fmt=style, capsize=3, label=label)
        lim = sorted({(s[3], s[8]) for s in summary if s[0] == kind and s[8]})
        if lim:
            ax.plot([p[0] for p in lim], [p[1] for p in lim], ":", color="gray", label="1 GbE network limit")
        ax.set_title(f"Aggregate {kind} throughput (median of runs)")
        ax.set_xlabel("concurrent clients")
        ax.set_ylabel("MB/s")
        ax.set_xticks(sorted({s[3] for s in summary}))
        ax.set_ylim(bottom=0)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "throughput.png"), dpi=150)
    print(f"\nwrote {os.path.join(out, 'throughput.png')}")


def recovery(raw, jobs, out):
    rows = []
    for job in jobs:
        d = os.path.join(raw, job)
        copy_raw(d, out, job)
        env = dict(l.strip().split("=", 1) for l in open(os.path.join(d, "env.txt")) if "=" in l and not l.startswith("=="))
        live = len(env["chunkservers"].split()) - int(env["KILL"].split()[0]) if "KILL" in env else 3
        kill = min(int(r[1]) for r in load(os.path.join(d, "raw", "kill.csv")))
        watch = load(os.path.join(d, "raw", "watch.csv"))
        rec = next((int(r[1]) for r in watch if r[0] == "recovered"), None)
        polls = [r for r in watch if r[0] != "recovered"]
        det = next((int(r[0]) for r in polls if int(r[1]) == live), None)
        peak_under = max(int(r[3]) for r in polls)
        chunks = int(polls[-1][2])
        hist_after_kill = next((r[6] for r in polls if int(r[1]) == live), "")
        clones = int(polls[-1][5])
        ver = load(os.path.join(d, "raw", "verify.csv"))
        vbytes = sum(int(r[3]) for r in ver)
        extra = {}
        for r in ver:
            for kv in r[6].split(";"):
                k, v = kv.split("=")
                extra[k] = extra.get(k, 0) + int(v)
        ds = load(os.path.join(d, "raw", "dataset.csv"))
        rows.append(dict(job=job, victims=env.get("victims", ""), data_GB=sum(int(r[3]) for r in ds) / 1e9, chunks=chunks,
                         under_replicated=peak_under, hist_after_kill=hist_after_kill, clones=clones,
                         detect_s=(det - kill) / 1e9 if det else None, rereplicate_s=(rec - det) / 1e9 if rec and det else None,
                         total_s=(rec - kill) / 1e9 if rec else None, clone_MBps=clones * 64 * 2**20 / MB / ((rec - det) / 1e9) if rec and det else None,
                         verified_GB=vbytes / 1e9, replica_reads=extra.get("replica_reads", 0), problems=extra.get("problems", 0),
                         verifiers=len(ver)))
    keys = list(rows[0].keys())
    with open(os.path.join(out, "recovery_runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: (f"{v:.2f}" if isinstance(v, float) else v) for k, v in r.items()})
    print("\n| job | victims | data GB | chunks | under-replicated | clones | kill→detected s | detected→3x s | kill→3x s | clone MB/s | verified GB | replica reads | problems |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        print(f"| {r['job']} | {r['victims']} | {r['data_GB']:.1f} | {r['chunks']} | {r['under_replicated']} | {r['clones']} | {fmt(r['detect_s'])} | {fmt(r['rereplicate_s'])} | {fmt(r['total_s'])} | {fmt(r['clone_MBps'])} | {r['verified_GB']:.1f} | {r['replica_reads']} | {r['problems']} |")
    for k in ("detect_s", "rereplicate_s", "total_s", "clone_MBps"):
        v = [r[k] for r in rows if r[k] is not None]
        if v:
            print(f"median {k}: {st.median(v):.1f} (n={len(v)}, min {min(v):.1f}, max {max(v):.1f})")


def extras(r):
    return dict(kv.split("=", 1) for kv in r[6].split(";") if "=" in kv) if len(r) > 6 else {}


def meta(raw, jobs, out):
    """Metadata ops/s: raw/meta-<op>-<layout>-n<N>-r<run>.csv, one line per
    client process (each running `threads` clients); bytes = operations."""
    runs = []
    for job in jobs:
        d = os.path.join(raw, job)
        copy_raw(d, out, job)
        for f in sorted(glob.glob(os.path.join(d, "raw", "meta-*.csv"))):
            m = re.fullmatch(r"meta-(create|stat|delete)-(private|shared)-n(\d+)-r(\d+)", os.path.basename(f)[:-4])
            if not m:
                continue
            rows = load(f)
            op, layout, n, run = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
            threads = sum(int(extras(r)["threads"]) for r in rows)
            if threads != n:
                print(f"skip {job}/{os.path.basename(f)}: {threads} of {n} clients finished")
                continue
            ops = sum(int(r[3]) for r in rows)
            secs = (max(int(r[5]) for r in rows) - min(int(r[4]) for r in rows)) / 1e9
            ex = [extras(r) for r in rows]
            runs.append(dict(job=job, op=op, layout=layout, clients=n, run=run, ops=ops, seconds=secs, ops_per_s=ops / secs,
                             errors=sum(int(e["errors"]) for e in ex),
                             # per-process percentiles cannot be merged exactly:
                             # median of the processes' p50s, worst process's p99
                             p50_us=st.median(float(e["p50_us"]) for e in ex), p99_us=max(float(e["p99_us"]) for e in ex)))
    with open(os.path.join(out, "meta_runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, list(runs[0].keys()))
        w.writeheader()
        for r in runs:
            w.writerow({k: (f"{v:.2f}" if isinstance(v, float) else v) for k, v in r.items()})
    groups = {}
    for r in runs:
        groups.setdefault((r["op"], r["layout"], r["clients"]), []).append(r)
    order = {"create": 0, "stat": 1, "delete": 2}
    summary = []
    for (op, layout, n), v in sorted(groups.items(), key=lambda kv: (order[kv[0][0]], kv[0][1], kv[0][2])):
        x = [r["ops_per_s"] for r in v]
        summary.append(dict(op=op, layout=layout, clients=n, runs=len(v), median_ops_per_s=st.median(x), min_ops_per_s=min(x), max_ops_per_s=max(x),
                            median_p50_us=st.median(r["p50_us"] for r in v), median_p99_us=st.median(r["p99_us"] for r in v),
                            ops_per_run=st.median(r["ops"] for r in v), errors=sum(r["errors"] for r in v)))
    with open(os.path.join(out, "meta_summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, list(summary[0].keys()))
        w.writeheader()
        for s in summary:
            w.writerow({k: (f"{v:.1f}" if isinstance(v, float) else v) for k, v in s.items()})
    print("\n| op | directory | clients | runs | median ops/s | min–max | p50 µs | p99 µs | ops/run | errors |")
    print("|---|---|---:|---:|---:|---|---:|---:|---:|---:|")
    for s in summary:
        print(f"| {s['op']} | {s['layout']} | {s['clients']} | {s['runs']} | {s['median_ops_per_s']:.0f} | {s['min_ops_per_s']:.0f}–{s['max_ops_per_s']:.0f} | "
              f"{s['median_p50_us']:.0f} | {s['median_p99_us']:.0f} | {s['ops_per_run']:.0f} | {s['errors']} |")
    plot_meta(summary, out)


def plot_meta(summary, out):
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    fig, ax = plt.subplots(figsize=(6.5, 4.2))
    for op, style in (("create", "o"), ("stat", "s"), ("delete", "^")):
        for layout, ls in (("private", "-"), ("shared", "--")):
            pts = sorted((s["clients"], s["median_ops_per_s"], s["min_ops_per_s"], s["max_ops_per_s"]) for s in summary if s["op"] == op and s["layout"] == layout)
            if pts:
                ax.errorbar([p[0] for p in pts], [p[1] for p in pts], yerr=[[p[1] - p[2] for p in pts], [p[3] - p[1] for p in pts]],
                            fmt=style + ls, capsize=3, label=f"{op}, {layout} directory")
    ax.set_xscale("log")
    xs = sorted({s["clients"] for s in summary})
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_title("Master metadata operations (median of runs)")
    ax.set_xlabel("concurrent clients")
    ax.set_ylabel("operations / s")
    ax.set_ylim(bottom=0)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "meta.png"), dpi=150)
    print(f"\nwrote {os.path.join(out, 'meta.png')}")


def snapshot(raw, jobs, out):
    """Snapshot vs. copy: raw/snapshot.csv from `gfs-bench snapshot`, plus
    raw/verify-snap.csv (every byte of one directory snapshot re-read)."""
    for job in jobs:
        d = os.path.join(raw, job)
        copy_raw(d, out, job)
        rows = load(os.path.join(d, "raw", "snapshot.csv"))
        # Run 1 snapshots start right after the dataset is written, while
        # leases from those writes are live and must be revoked; later runs
        # come minutes later, after every 60 s lease has expired. (The
        # many-files rows say so themselves.)
        for r in rows:
            if r[0] == "snapshot" and "leases=" not in r[6]:
                r[6] += ";leases=" + ("revoked" if r[1] == "1" else "none")
        ms = lambda r: (int(r[5]) - int(r[4])) / 1e6
        print(f"\njob {job}")
        print("\n| operation | what | runs | data | median ms | min–max ms | MB/s |")
        print("|---|---|---:|---:|---:|---|---:|")
        summary = []
        for kind, what in (("snapshot", "file"), ("snapshot", "dir"), ("snapshot", "many"), ("copy", "file"), ("copy", "dir")):
            for leases in sorted({extras(r).get("leases", "") for r in rows if r[0] == kind and extras(r).get("what") == what}):
                v = [r for r in rows if r[0] == kind and extras(r).get("what") == what and extras(r).get("leases", "") == leases]
                t = [ms(r) for r in v]
                b = int(v[0][3])
                label = what + (f" (leases {leases})" if leases else "")
                # a snapshot moves no data, so a data rate means nothing for it
                rate = st.median(int(r[3]) / MB / (ms(r) / 1e3) for r in v) if kind == "copy" else ""
                summary.append(dict(op=kind, what=label, runs=len(v), bytes=b, median_ms=st.median(t), min_ms=min(t), max_ms=max(t), MBps=rate))
                print(f"| {kind} | {label} | {len(v)} | {b / 1e9:.2f} GB | {st.median(t):.1f} | {min(t):.1f}–{max(t):.1f} | {rate and f'{rate:.0f}'} |")
        med = {(x["op"], x["what"]): x["median_ms"] for x in summary}
        for what in ("file", "dir"):
            for leases in ("revoked", "none"):
                k = ("snapshot", f"{what} (leases {leases})")
                if k in med and ("copy", what) in med:
                    print(f"{what}: copy takes {med[('copy', what)] / med[k]:,.0f}x as long as a snapshot with leases {leases}")
        print("\n| copy-on-write phase | writes | median ms | min–max ms |")
        print("|---|---:|---:|---|")
        for phase in ("shared-first", "rewrite", "unshared-first"):
            t = [ms(r) for r in rows if r[0] == "cow" and extras(r).get("phase") == phase]
            if t:
                summary.append(dict(op="cow", what=phase, runs=len(t), bytes=4096, median_ms=st.median(t), min_ms=min(t), max_ms=max(t), MBps=""))
                print(f"| {phase} | {len(t)} | {st.median(t):.1f} | {min(t):.1f}–{max(t):.1f} |")
        for r in rows:
            if r[0] == "check":
                print(f"\ncontent check: {int(r[3]) / 1e9:.2f} GB read, {r[6]}")
        vf = os.path.join(d, "raw", "verify-snap.csv")
        if os.path.exists(vf):
            ver = load(vf)
            tot = {}
            for r in ver:
                for k, v in extras(r).items():
                    tot[k] = tot.get(k, 0) + int(v)
            print(f"verify of a directory snapshot: {len(ver)} verifiers, {sum(int(r[3]) for r in ver) / 1e9:.2f} GB, {tot}")
        with open(os.path.join(out, "snapshot_summary.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, list(summary[0].keys()))
            w.writeheader()
            for s in summary:
                w.writerow({k: (f"{v:.1f}" if isinstance(v, float) else v) for k, v in s.items()})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="context/benchmarks/raw")
    ap.add_argument("--out", default="bench/results")
    ap.add_argument("--throughput", nargs="*", default=[])
    ap.add_argument("--recovery", nargs="*", default=[])
    ap.add_argument("--meta", nargs="*", default=[])
    ap.add_argument("--snapshot", nargs="*", default=[])
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.throughput:
        throughput(a.raw, a.throughput, a.out)
    if a.recovery:
        recovery(a.raw, a.recovery, a.out)
    if a.meta:
        meta(a.raw, a.meta, a.out)
    if a.snapshot:
        snapshot(a.raw, a.snapshot, a.out)


if __name__ == "__main__":
    main()
