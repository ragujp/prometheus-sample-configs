#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EC2 Reachability (IPv4 / IPv6) のデータを公式サイトのJSONから取得して
Prometheus HTTP SD 形式の JSON を出力します。

出力: out/aws/aws-targets.json（IPv4/IPv6統合）
オプション出力: out/aws/aws-targets-ipv4.json, out/aws/aws-targets-ipv6.json
"""

import json
import sys
import time
import pathlib
from typing import List, Dict, Tuple
import requests

BASE_URL  = "http://ec2-reachability.amazonaws.com"
IPV6_BASE = "http://ipv6.ec2-reachability.amazonaws.com"

REGIONS_URL  = f"{BASE_URL}/regions.json"
IPv4_URL     = f"{BASE_URL}/prefixes-ipv4.json"
IPv6_URL     = f"{IPV6_BASE}/prefixes-ipv6.json"

UA = "Mozilla/5.0 (+Prometheus-HTTP-SD-AWS-Reachability; contact=ops@example.com)"

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out" / "aws"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_ALL = OUT_DIR / "aws-targets.json"
OUT_V4  = OUT_DIR / "aws-targets-ipv4.json"
OUT_V6  = OUT_DIR / "aws-targets-ipv6.json"


def fetch_json(url: str, retries: int = 3, timeout: int = 20) -> object:
    ex = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            ex = e
            time.sleep(1.5)
    raise RuntimeError(f"fetch failed: {url} ({ex})")


def split_city_country(name: str) -> Tuple[str, str]:
    """'City, Country' 形式を (city, country) に分割する。"""
    if "," in name:
        a, b = [t.strip() for t in name.split(",", 1)]
        return a, b
    return name, ""


def parse_prefixes(data: List[Dict], regions: Dict, ip_version: str) -> List[Dict]:
    """
    prefixes-ipv4.json / prefixes-ipv6.json の形式:
      [ { "region-code": { "cidr": "ip", ... } }, ... ]
    """
    out = []
    for obj in data:
        for region_code, prefix_map in obj.items():
            region_info = regions.get(region_code, {})
            area    = region_info.get("geo", "")
            city, country = split_city_country(region_info.get("name", region_code))

            for _prefix, ip in prefix_map.items():
                labels = {
                    "provider":   "aws",
                    "area":       area,
                    "region":     region_code,
                    "city":       city,
                    "country":    country,
                    "ip_version": "6" if ip_version == "v6" else "4",
                    "source":     "ec2-reachability",
                }
                out.append({"targets": [ip], "labels": labels})
    return out


def sort_key(g: Dict) -> Tuple:
    L = g.get("labels", {})
    return (L.get("area", ""), L.get("region", ""), L.get("city", ""), g["targets"][0], L.get("ip_version", ""))


def main():
    print("[*] Fetching regions metadata…", file=sys.stderr)
    regions = fetch_json(REGIONS_URL)

    print("[*] Fetching IPv4 data…", file=sys.stderr)
    v4_data = fetch_json(IPv4_URL)

    print("[*] Fetching IPv6 data…", file=sys.stderr)
    v6_data = fetch_json(IPv6_URL)

    print("[*] Parsing…", file=sys.stderr)
    v4 = parse_prefixes(v4_data, regions, ip_version="v4")
    v6 = parse_prefixes(v6_data, regions, ip_version="v6")

    def dedup(lst: List[Dict]) -> List[Dict]:
        seen = set()
        uniq = []
        for g in lst:
            key = (tuple(g.get("targets", [])), tuple(sorted(g.get("labels", {}).items())))
            if key not in seen:
                seen.add(key)
                uniq.append(g)
        return uniq

    v4 = dedup(v4)
    v6 = dedup(v6)

    all_groups  = sorted(v4 + v6, key=sort_key)
    v4_sorted   = sorted(v4, key=sort_key)
    v6_sorted   = sorted(v6, key=sort_key)

    print(f"[*] IPv4 entries: {len(v4_sorted)}  IPv6 entries: {len(v6_sorted)}  Total: {len(all_groups)}", file=sys.stderr)

    OUT_ALL.write_text(json.dumps(all_groups, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_V4.write_text(json.dumps(v4_sorted,  ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_V6.write_text(json.dumps(v6_sorted,  ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[*] wrote: {OUT_ALL}", file=sys.stderr)
    print(f"[*] wrote: {OUT_V4}", file=sys.stderr)
    print(f"[*] wrote: {OUT_V6}", file=sys.stderr)


if __name__ == "__main__":
    main()
