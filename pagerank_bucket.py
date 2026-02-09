#!/usr/bin/env python3

import argparse
import re
import time
from collections import defaultdict
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


def summarize(values: List[int]) -> Dict:
    if not values:
        return {"count": 0, "min": 0, "max": 0, "avg": 0.0, "median": 0.0, "quintiles": [0, 0, 0, 0, 0, 0]}
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


def list_page_indices(client: storage.Client, bucket_name: str, prefix: str) -> List[int]:
    idxs = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        name = blob.name
        if name.endswith("/"):
            continue
        base = name.rsplit("/", 1)[-1]
        if base.endswith(".html"):
            stem = base[:-5]
            if stem.isdigit():
                idxs.append(int(stem))
    idxs.sort()
    return idxs


def download_pages_build_graph(
    bucket_name: str,
    prefix: str,
    n_expected: int | None,
) -> Tuple[int, Dict[int, List[int]], Dict[int, int], float]:
    """
    Returns:
      n_pages,
      outlinks: page_id -> list[dest_id] (filtered to 0..n_pages-1),
      indeg: dest_id -> incoming count,
      seconds_read
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    t0 = time.time()
    idxs = list_page_indices(client, bucket_name, prefix)
    if n_expected is not None:
        if len(idxs) < n_expected:
            pass
        n_pages = n_expected
    else:
        n_pages = (max(idxs) + 1) if idxs else 0

    outlinks: Dict[int, List[int]] = {}
    indeg: Dict[int, int] = defaultdict(int)

   
    for i in range(n_pages):
        blob_name = f"{prefix.rstrip('/')}/{i}.html"
        blob = bucket.blob(blob_name)
        try:
            html = blob.download_as_text()
        except Exception:
            html = ""

        outs = parse_outgoing_ids(html)
        outs = [d for d in outs if 0 <= d < n_pages]
        outlinks[i] = outs
        for d in outs:
            indeg[d] += 1

    t1 = time.time()
    return n_pages, outlinks, indeg, (t1 - t0)


def pagerank_iterative(
    n: int,
    outlinks: Dict[int, List[int]],
    d: float = 0.85,
    tol: float = 0.005,
    max_iter: int = 200,
) -> Tuple[List[float], int, float]:
    
    if n <= 0:
        return [], 0, 0.0

    incoming: List[List[int]] = [[] for _ in range(n)]
    outdeg = [0] * n
    for src in range(n):
        outs = outlinks.get(src, [])
        outdeg[src] = len(outs)
        for dst in outs:
            incoming[dst].append(src)

    pr = [1.0 / n] * n
    base = (1.0 - d) / n

    start = time.time()
    prev_sum = sum(pr)

    for it in range(1, max_iter + 1):
        dangling_mass = sum(pr[i] for i in range(n) if outdeg[i] == 0)
        new_pr = [0.0] * n

        for a in range(n):
            s = 0.0
            for t in incoming[a]:
                c = outdeg[t]
                if c > 0:
                    s += pr[t] / c
            s += dangling_mass / n
            new_pr[a] = base + d * s

        delta = sum(abs(new_pr[i] - pr[i]) for i in range(n))
        denom = prev_sum if prev_sum != 0 else 1.0
        if (delta / denom) <= tol:
            end = time.time()
            return new_pr, it, (end - start)

        pr = new_pr
        prev_sum = sum(pr)

    end = time.time()
    return pr, max_iter, (end - start)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True, help='Example: "pages/" (objects like pages/0.html)')
    ap.add_argument("--n", type=int, default=20000, help="Number of pages expected (default 20000).")
    ap.add_argument("--topk", type=int, default=5)
    args = ap.parse_args()

    t_all0 = time.time()
    n, outlinks, indeg, read_s = download_pages_build_graph(args.bucket, args.prefix, args.n)
    t_after_load = time.time()

    out_counts = [len(outlinks[i]) for i in range(n)]
    in_counts = [int(indeg.get(i, 0)) for i in range(n)]

    in_stats = summarize(in_counts)
    out_stats = summarize(out_counts)

    pr, iters, pr_s = pagerank_iterative(n, outlinks, d=0.85, tol=0.005)
    t_all1 = time.time()

    top = sorted([(i, pr[i]) for i in range(n)], key=lambda x: x[1], reverse=True)[: args.topk]

    print(f"PAGES: {n}")
    print(f"READ_SECONDS: {read_s:.3f}")
    print(f"PAGERANK_SECONDS: {pr_s:.3f}")
    print(f"TOTAL_SECONDS: {(t_all1 - t_all0):.3f}")
    print(f"PAGERANK_ITERS: {iters}")

    print("\nINCOMING_LINKS_STATS:")
    print(in_stats)
    print("\nOUTGOING_LINKS_STATS:")
    print(out_stats)

    print("\nTOP_PAGES_BY_PAGERANK:")
    for pid, score in top:
        print(f"{pid}.html\t{score:.10f}")


if __name__ == "__main__":
    main()
