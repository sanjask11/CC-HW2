#!/usr/bin/env python3
import argparse
import random
import sys
import time
from urllib import request, error

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dest", required=True, help="Load balancer IP or hostname")
    parser.add_argument("-p", "--port", type=int, default=8080)
    parser.add_argument("-w", "--prefix", default="", help="Optional path prefix")
    parser.add_argument("-n", "--num", type=int, default=60, help="Number of requests")
    parser.add_argument("-r", "--range", dest="upper", type=int, default=100, help="Random file upper bound")
    parser.add_argument("--fixed", default=None, help="Fixed path like 0.html")
    args = parser.parse_args()

    prefix = args.prefix.strip("/")
    zone_counts = {}
    errors = 0

    for i in range(args.num):
        if args.fixed:
            path = args.fixed.lstrip("/")
        else:
            path = f"{random.randint(0, max(args.upper - 1, 0))}.html"

        if prefix:
            url = f"http://{args.dest}:{args.port}/{prefix}/{path}"
        else:
            url = f"http://{args.dest}:{args.port}/{path}"

        ts = time.strftime("%H:%M:%S")
        try:
            with request.urlopen(url, timeout=5) as resp:
                zone = resp.headers.get("X-Zone", "missing")
                status = resp.status
                zone_counts[zone] = zone_counts.get(zone, 0) + 1
                print(f"[{ts}] {status} zone={zone} url={url}")
        except error.HTTPError as e:
            zone = e.headers.get("X-Zone", "missing")
            errors += 1
            print(f"[{ts}] HTTP {e.code} zone={zone} url={url}")
        except Exception as e:
            errors += 1
            print(f"[{ts}] ERROR {e} url={url}")

        time.sleep(1)

    print("\nSummary")
    total_ok = sum(zone_counts.values())
    print(f"successful_responses={total_ok}")
    print(f"errors={errors}")
    for zone, count in sorted(zone_counts.items()):
        print(f"{zone}: {count}")

if __name__ == "__main__":
    main()