#!/usr/bin/env python3

import argparse
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import median
from typing import Dict, List, Tuple

import numpy as np
from google.cloud import storage

HREF_RE = re.compile(r'<a\s+HREF="(\d+)\.html"', re.IGNORECASE)


def percentile_quintiles(values: List[int]) -> List[float]:
    if not values:
        return [0, 0, 0, 0, 0, 0]
    arr = np.asarray(values, dtype=float)
    return np.percentile(arr, [0, 20, 40, 60, 80, 100]).tolist()


def summarize(values: List[int]) -> Dict[str, object]:
    if not values:
        return {
            "count": 0,
            "min": 0,
            "max": 0,
            "avg": 0.0,
            "median": 0.0,
            "quintiles": [0, 0, 0, 0, 0, 0],
        }
    return {
        "count": len(values),
        "min": int(min(values)),
        "max": int(max(values)),
        "avg": float(sum(values) / len(values)),
        "median": float(median(values)),
        "quintiles": percentile_quintiles(values),
    }


def parse_outgoing_ids(html: str) -> List[int]:
    return [int(x) for x in HREF_RE.findall(html)]


def download_pages_build_graph_parallel(
    bucket_name: str,
    prefix: str,
    n_pages: int,
    workers: int,
) -> Tuple[Dict[int, List[int]], Dict[int, int], float]:
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = prefix.rstrip("/")

    def fetch_one(i: int) -> Tuple[int, List[int]]:
        blob_name = f"{prefix}/{i}.html"
        html = bucket.blob(blob_name).download_as_text()
        outs = parse_outgoing_ids(html)
        outs = [d for d in outs if 0 <= d < n_pages]
        return i, outs

    t0 = time.time()
    outlinks: Dict[int, List[int]] = {}
    indeg: Dict[int, int] = defaultdict(int)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(fetch_one, i) for i in range(n_pages)]
        for fut in as_completed(futures):
            i, outs = fut.result()
            outlinks[i] = outs

    for i in range(n_pages):
        for d in outlinks.get(i, []):
            indeg[d] += 1

    t1 = time.time()
    return outlinks, indeg, (t1 - t0)


def pagerank_iterative_hw_stop(
    n: int,
    outlinks: Dict[int, List[int]],
    max_iter: int = 200,
) -> Tuple[List[float], int, float]:
   
    if n <= 0:
        return [], 0, 0.0

    d = 0.85
    base = (1.0 - d) / n

    incoming: List[List[int]] = [[] for _ in range(n)]
    outdeg = [0] * n
    for src in range(n):
        outs = outlinks.get(src, [])
        outdeg[src] = len(outs)
        for dst in outs:
            incoming[dst].append(src)

    pr = [1.0 / n] * n

    start = time.time()
    for it in range(1, max_iter + 1):
        sum_old = sum(pr)
        new_pr = [0.0] * n

        for a in range(n):
            s = 0.0
            for t in incoming[a]:
                c = outdeg[t]
                if c > 0:
                    s += pr[t] / c
            new_pr[a] = base + d * s

        sum_new = sum(new_pr)
        denom = sum_old if sum_old != 0 else 1.0
        if abs(sum_new - sum_old) / denom <= 0.005:
            end = time.time()
            return new_pr, it, (end - start)

        pr = new_pr

    end = time.time()
    return pr, max_iter, (end - start)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True, help='Example: "html-pages/"')
    ap.add_argument("--n", type=int, default=20000, help="Number of pages (default 20000).")
    ap.add_argument("--topk", type=int, default=5)
    ap.add_argument("--workers", type=int, default=32, help="Parallel download worker threads.")
    ap.add_argument("--max-iter", type=int, default=200)
    args = ap.parse_args()

    t_all0 = time.time()

    outlinks, indeg, read_s = download_pages_build_graph_parallel(
        args.bucket, args.prefix, args.n, args.workers
    )

    out_counts = [len(outlinks.get(i, [])) for i in range(args.n)]
    in_counts = [int(indeg.get(i, 0)) for i in range(args.n)]

    in_stats = summarize(in_counts)
    out_stats = summarize(out_counts)

    pr, iters, pr_s = pagerank_iterative_hw_stop(args.n, outlinks, max_iter=args.max_iter)

    t_all1 = time.time()

    top = sorted(((i, pr[i]) for i in range(args.n)), key=lambda x: x[1], reverse=True)[: args.topk]

    print(f"PAGES: {args.n}")
    print(f"READ_SECONDS: {read_s:.3f}")
    print(f"PAGERANK_SECONDS: {pr_s:.3f}")
    print(f"TOTAL_SECONDS: {(t_all1 - t_all0):.3f}")
    print(f"PAGERANK_ITERS: {iters}")
    print(f"PAGERANK_SUM: {sum(pr):.10f}")

    print("\nINCOMING_LINKS_STATS:")
    print(in_stats)
    print("\nOUTGOING_LINKS_STATS:")
    print(out_stats)

    print("\nTOP_PAGES_BY_PAGERANK:")
    for pid, score in top:
        print(f"{pid}.html\t{score:.10f}")


if __name__ == "__main__":
    main()
