#!/usr/bin/env python3
import json, os, socket, sys
from urllib.request import Request, urlopen

OOKLA_API = "https://www.speedtest.net/api/js/servers?engine=js&search=Japan&limit=100"
OUT_DIR = "out"
HDRS = {"User-Agent": "github-actions-ookla-targets/1.0"}

def fetch_json(url: str):
    with urlopen(Request(url, headers=HDRS), timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def strip_port(hostport: str) -> str:
    # e.g. "host.example.net:8080" -> "host.example.net"
    return hostport.rsplit(":", 1)[0]

def resolve_addrs(host: str):
    v4, v6 = set(), set()
    for fam in (socket.AF_INET, socket.AF_INET6):
        try:
            infos = socket.getaddrinfo(host, None, fam, socket.SOCK_STREAM)
            for info in infos:
                ip = info[4][0]
                (v4 if fam == socket.AF_INET else v6).add(ip)
        except socket.gaierror:
            # 無視（片系のみ解決できないケースあり）
            pass
    return sorted(v4), sorted(v6)

def make_group(targets, labels):
    return {"targets": targets, "labels": labels}

def sort_groups(groups):
    # 差分を安定化するために並べ替え
    def key(g):
        L = g.get("labels", {})
        return (
            L.get("city",""),
            L.get("sponsor",""),
            L.get("ookla_id",""),
            L.get("ip_family",""),
            L.get("fqdn",""),
            L.get("url",""),
            tuple(g.get("targets",[])),
        )
    return sorted(groups, key=key)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    data = fetch_json(OOKLA_API)

    icmp_v4, icmp_v6 = [], []

    for item in data:
        # Japan 以外は除外
        if item.get("country") != "Japan":
            continue

        host_field = item.get("host")     # 例: "foo.prod.hosts.ooklaserver.net:8080"
        url = item.get("url") or ""       # 通知用ラベルで使う
        if not host_field:
            continue

        fqdn = strip_port(host_field)
        v4_list, v6_list = resolve_addrs(fqdn)

        base = {
            "ookla_id": str(item.get("id","")),
            "city": item.get("name",""),
            "sponsor": item.get("sponsor",""),
            "cc": item.get("cc",""),
            "fqdn": fqdn,
            "url": url,  # 差分通知で URL を使う
        }

        if v4_list:
            icmp_v4.append(make_group(v4_list, {**base, "ip_family":"v4"}))
        if v6_list:
            icmp_v6.append(make_group(v6_list, {**base, "ip_family":"v6"}))

    def dump(name, groups):
        path = os.path.join(OUT_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sort_groups(groups), f, ensure_ascii=False, indent=2)
            f.write("\n")

    dump("ookla_icmp_targets_ipv4.json", icmp_v4)
    dump("ookla_icmp_targets_ipv6.json", icmp_v6)

    print(
        f"[ookla] icmp(v4_groups={len(icmp_v4)}, v6_groups={len(icmp_v6)})",
        file=sys.stderr,
    )

if __name__ == "__main__":
    main()
