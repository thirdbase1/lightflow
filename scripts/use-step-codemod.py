#!/usr/bin/env python3
"""Wrap "use step" functions with makeStep() for lightflow durability.

Two-phase rebuild: collect spans for every directive's innermost enclosing
function, then rebuild the file once (no incremental string appends, so no
duplicated bodies). Handles:
  - `async function NAME(...): Ret {` (incl. object/generic return types)
  - `export async function NAME(...) {`
Skips directives whose enclosing form isn't a plain function declaration
(arrow consts are handled by scripts/post-codemod.py).
"""
import re
import sys
import pathlib


def skip_string(src, j):
    instr = None
    n = len(src)
    while j < n:
        c = src[j]
        if instr:
            if c == "\\":
                j += 2
                continue
            if instr == "`" and c == "$" and j + 1 < n and src[j + 1] == "{":
                k = skip_braces(src, src.index("{", j + 1) if j + 1 < n else -1)
                j = (n if k < 0 else k) + 1
                continue
            if c == instr:
                return j + 1
            j += 1
            continue
        if c in "\"`":
            instr = c
        j += 1
    return n


def skip_braces(src, open_pos):
    """Return index of the '}' matching the '{' at open_pos (-1 if unmatched)."""
    d = 0
    j = open_pos
    n = len(src)
    while j < n:
        c = src[j]
        if c in "\"`":
            j = skip_string(src, j)
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "/":
            k = src.find("\n", j)
            j = n if k < 0 else k
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "*":
            e = src.find("*/", j)
            j = n if e < 0 else e + 2
            continue
        if c == "{":
            d += 1
        elif c == "}":
            d -= 1
            if d == 0:
                return j
        j += 1
    return -1


def paren_close(src, open_pos):
    d = 0
    j = open_pos
    n = len(src)
    while j < n:
        c = src[j]
        if c in "\"`":
            j = skip_string(src, j)
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "/":
            k = src.find("\n", j)
            j = n if k < 0 else k
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "*":
            e = src.find("*/", j)
            j = n if e < 0 else e + 2
            continue
        if c == "(":
            d += 1
        elif c == ")":
            d -= 1
            if d == 0:
                return j
        j += 1
    return -1


def body_open(src, pc, directive_pos):
    """First '{' after the arg list that is actually the body brace."""
    j = pc + 1
    n = len(src)
    while j < n:
        c = src[j]
        if c in "\"`":
            j = skip_string(src, j)
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "/":
            k = src.find("\n", j)
            j = n if k < 0 else k
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "*":
            e = src.find("*/", j)
            j = n if e < 0 else e + 2
            continue
        if c == "{":
            close = skip_braces(src, j)
            if close >= directive_pos:
                return j
            j = close + 1  # a brace in the return type
            continue
        if c in "([":
            close = skip_braces(src, j)
            if close < 0:
                return -1
            j = close + 1
            continue
        if c == ";":
            return -1
        j += 1
    return -1


FN = re.compile(r"(export\s+)?(async\s+)?function\s+(\w+)\s*\(")

for f in sys.argv[1:]:
    p = pathlib.Path(f)
    src = p.read_text()
    if '"use step"' not in src:
        continue
    spans = []
    for m in re.finditer(r'"use step";', src):
        best = None
        for hm in reversed(list(FN.finditer(src, max(0, m.start() - 60000), m.start()))):
            pc = paren_close(src, hm.end() - 1)
            if pc < 0 or pc > m.start():
                continue
            bo = body_open(src, pc, m.end())
            if bo < 0 or bo > m.start():
                continue
            be = skip_braces(src, bo)
            if be >= m.end():
                best = (hm, bo, be)
                break
        if best is None:
            continue
        hm, bo, be = best
        export = hm.group(1) or ""
        asy = hm.group(2) or ""
        name = hm.group(3)
        spans.append((hm.start(), hm.end(), bo, be, export, asy, name))
    if not spans:
        continue
    seen = set()
    uniq = []
    for s in sorted(spans):
        if s[0] in seen:
            continue
        seen.add(s[0])
        uniq.append(s)
    out = []
    prev = 0
    for (hs, he, bo, be, export, asy, name) in uniq:
        out.append(src[prev:hs])
        pc = paren_close(src, he - 1)
        out.append(f"{export}const {name} = makeStep({asy}function {name}{src[he-1:pc+1]}{src[pc+1:bo]}{{")
        out.append(src[bo + 1:be])
        out.append("});")
        prev = be + 1
    out.append(src[prev:])
    new = "".join(out)
    lines = new.split("\n")
    imp = 'import { makeStep } from "lightflow-engine/compat/workflow";'
    for idx, ln in enumerate(lines[:60]):
        if ln.startswith("import "):
            lines.insert(idx, imp)
            break
    p.write_text("\n".join(lines))
    print(f"{p.name}: wrapped {len(uniq)}")
