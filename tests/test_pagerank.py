from pagerank_bucket import pagerank_iterative


def test_pagerank_invariants_and_order():
    n = 4
    outlinks = {
        0: [1, 2],
        1: [2],
        2: [0],
        3: [2],
    }

    pr, iters, _ = pagerank_iterative(n, outlinks, d=0.85, tol=1e-10, max_iter=1000)

    assert iters >= 1
    assert len(pr) == n
    assert all(v > 0 for v in pr)

    s = sum(pr)
    assert abs(s - 1.0) < 1e-6


    order = sorted(range(n), key=lambda i: pr[i], reverse=True)
    assert order[0] == 2
    assert order[-1] == 3
