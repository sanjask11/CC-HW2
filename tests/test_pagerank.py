from pagerank_bucket import pagerank_iterative_hw_stop


def test_pagerank_sum_is_oneish():
    n = 4
    outlinks = {
        0: [1, 2],
        1: [2],
        2: [0],
        3: [2],
    }

    pr, iters, _ = pagerank_iterative_hw_stop(n, outlinks, max_iter=2000)

    assert iters >= 1
    assert len(pr) == n
    assert all(v >= 0 for v in pr)

    s = sum(pr)
    # Should be close to 1; allow tiny numeric drift
    assert abs(s - 1.0) < 1e-3
