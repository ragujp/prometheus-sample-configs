#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EC2 Reachability (IPv4 / IPv6) の表を公式サイトからスクレイピングして
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
from bs4 import BeautifulSoup

IPv4_URL = "http://ec2-reachability.amazonaws.com/"
IPv6_URL = "http://ipv6.ec2-reachability.amazonaws.com/"

UA = "Mozilla/5.0 (+Prometheus-HTTP-SD-AWS-Reachability; contact=ops@example.com)"

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out" / "aws"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_ALL = OUT_DIR / "aws-targets.json"
OUT_V4  = OUT_DIR / "aws-targets-ipv4.json"
OUT_V6  = OUT_DIR / "aws-targets-ipv6.json"


def fetch(url: str, retries: int = 3, timeout: int = 20) -> str:
    ex = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            ex = e
            time.sleep(1.5)
    raise RuntimeError(f"fetch failed: {url} ({ex})")


def clean_text(s: str) -> str:
    return " ".join((s or "").strip().replace("\xa0", " ").split())


def split_city_country(head: str) -> Tuple[str, str]:
    txt = clean_text(head)
    if "," in txt:
        a, b = [t.strip() for t in txt.split(",", 1)]
        return a, b
    return txt, ""


def parse_table(panel, ip_version: str) -> List[Dict]:
    area = clean_text(panel.select_one(".panel-title").get_text()) if panel.select_one(".panel-title") else ""
    table = panel.select_one("table")
    if table is None:
        return []

    out = []
    current_city = ""
    current_country = ""

    for tr in table.select("tr"):
        th = tr.find("th", {"class": "region-heading"})
        if th:
            current_city, current_country = split_city_country(th.get_text())
            continue

        tds = tr.find_all("td")
        if len(tds) >= 3:
            region = clean_text(tds[0].get_text())
            ip = clean_text(tds[2].get_text())
            if not ip or ip.lower() in {"ip", "instance ip"}:
                continue

            labels = {
                "provider": "aws",
                "area": area,
                "region": region,
                "city": current_city,
                "country": current_country,
                "ip_version": "6" if ip_version == "v6" else "4",
                "source": "ec2-reachability",
            }
            out.append({"targets": [ip], "labels": labels})
    return out


def parse_ipv4(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    panels = soup.select(".panel.panel-default")
    results: List[Dict] = []
    for p in panels:
        results.extend(parse_table(p, ip_version="v4"))
    return results


def parse_ipv6(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    panels = soup.select(".panel.panel-default")
    results: List[Dict] = []
    for p in panels:
        results.extend(parse_table(p, ip_version="v6"))
    return results


def sort_key(g: Dict) -> Tuple:
    L = g.get("labels", {})
    return (L.get("area", ""), L.get("region", ""), L.get("city", ""), g["targets"][0], L.get("ip_version", ""))


def main():
    print("[*] Fetching IPv4 page…", file=sys.stderr)
    v4_html = fetch(IPv4_URL)

    print("[*] Fetching IPv6 page…", file=sys.stderr)
    v6_html = fetch(IPv6_URL)

    print("[*] Parsing…", file=sys.stderr)
    v4 = parse_ipv4(v4_html)
    v6 = parse_ipv6(v6_html)

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

    all_groups = sorted(v4 + v6, key=sort_key)
    v4_sorted = sorted(v4, key=sort_key)
    v6_sorted = sorted(v6, key=sort_key)

    print(f"[*] IPv4 entries: {len(v4_sorted)}  IPv6 entries: {len(v6_sorted)}  Total: {len(all_groups)}", file=sys.stderr)

    OUT_ALL.write_text(json.dumps(all_groups, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    OUT_V4.write_text(json.dumps(v4_sorted, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    OUT_V6.write_text(json.dumps(v6_sorted, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")

    print(f"[*] wrote: {OUT_ALL}", file=sys.stderr)
    print(f"[*] wrote: {OUT_V4}", file=sys.stderr)
    print(f"[*] wrote: {OUT_V6}", file=sys.stderr)


if __name__ == "__main__":
    main()
