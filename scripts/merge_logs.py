#!/usr/bin/env python3
"""Junta logs/node*.log em ordem cronologica pelo prefixo [   t.tttt]."""
import glob
import re

pat = re.compile(r"^\[\s*([0-9]+\.[0-9]+)\]")
rows = []
for path in sorted(glob.glob("logs/node*.log")):
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = pat.match(line)
            t = float(m.group(1)) if m else 0.0
            rows.append((t, line.rstrip("\n")))
rows.sort(key=lambda r: r[0])
for _, line in rows:
    print(line)
