import random, time, json, os
from datetime import datetime, timedelta, UTC
from pathlib import Path

out_dir = Path("sample_logs")
out_dir.mkdir(exist_ok=True, parents=True)
FORMAT = "clf"  
endpoints = ["/", "/login", "/signup", "/api/orders", "/api/cart", "/health"]
statuses  = [200, 200, 200, 200, 200, 500, 502, 404, 503]
methods   = ["GET", "POST"]
useragents = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "curl/8.0.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
]
hosts = ["10.0.0.1", "10.0.0.2", "172.16.5.9", "192.168.1.10"]

def make_line(now):
    host = random.choice(hosts)
    user = "-"
    dt   = now.strftime("%d/%b/%Y:%H:%M:%S +0000")
    method = random.choice(methods)
    path = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(100, 5000)
    ua = random.choice(useragents)
    latency_ms = random.randint(10, 1200)

    if FORMAT == "clf":
        return f'{host} - {user} [{dt}] "{method} {path} HTTP/1.1" {status} {size} "-" "{ua}" {latency_ms}'
    else:
        obj = {
            "ts": now.isoformat() + "Z",
            "host": host,
            "method": method,
            "path": path,
            "status": status,
            "bytes": size,
            "ua": ua,
            "latency_ms": latency_ms,
            "request_id": f"req_{random.randint(100000,999999)}"
        }
        return json.dumps(obj)

if __name__ == "__main__":
    lines = 5000
    from datetime import datetime, UTC
    now = datetime.now(UTC)
    out_file = out_dir / ("web.log" if FORMAT == "clf" else "web.jsonl")
    with out_file.open("w") as f:
        for i in range(lines):
            ts = now - timedelta(seconds=(lines - i))
            f.write(make_line(ts) + "\n")
    print(f"Created {out_file} with {lines} lines in {FORMAT} format.")
