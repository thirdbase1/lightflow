import re

def paren_close(src, open_pos):
    d=0; j=open_pos; n=len(src); instr=None
    while j<n:
        c=src[j]
        if instr:
            if c=="\\":
                j+=2; continue
            if c==instr: instr=None
            j+=1; continue
        if c in "\"`": instr=c; j+=1; continue
        if c=="(": d+=1
        elif c==")":
            d-=1
            if d==0: return j
        j+=1
    return -1

def find_body_end(src, open_pos):
    d=0; j=open_pos; n=len(src); stack=[]; instr=None
    while j<n:
        c=src[j]
        if instr:
            if c=="\\":
                j+=2; continue
            if instr=="`" and c=="$" and j+1<n and src[j+1]=="{":
                stack.append(True); instr=None; j+=2; continue
            if c==instr: instr=None
            j+=1; continue
        if c in "\"`": instr=c; j+=1; continue
        if c=="/" and j+1<n and src[j+1]=="/":
            k=src.find("\n",j); j=n if k<0 else k; continue
        if c=="/" and j+1<n and src[j+1]=="*":
            e=src.find("*/",j); j=n if e<0 else e+2; continue
        if c=="}" and stack and stack[-1]:
            stack.pop(); instr="`"; j+=1; continue
        if c=="{": d+=1
        elif c=="}":
            d-=1
            if d==0: return j
        j+=1
    return -1

for f in ["chat.ts","sandbox-lifecycle.ts","run-benchmarks.ts","chat-post-finish.ts","archive-sandbox-stop.ts","sandbox-provisioning.ts","chat-sandbox-runtime.ts"]:
    path = "/tmp/entry/apps/web/app/workflows/" + f
    src = open(path).read()
    spans = []
    for m in re.finditer(r"const \w+ = makeStep\(", src):
        pc = paren_close(src, src.index("(", m.end()))
        # body '{' is the depth-0 '{' after pc (skip return type braces)
        j = pc+1; d=0
        while j < len(src):
            c = src[j]
            if c == "{":
                cl = find_body_end(src, j)
                # if this close is followed by nothing weird and there is ANOTHER { before... treat type braces: continue if a ')' ';' or '=>' after close? simpler: body is the LAST depth-0 '{' before next 'const'/EOF — use: if close+1 < len and next non-ws is ';' → type brace, skip
                nxt = src[cl+1:cl+3].strip()
                if nxt == ");" or nxt == ")":
                    j = cl+1; continue
                spans.append((m.start(), cl)); break
            j += 1
    missed = []
    for m in re.finditer(r'^\s*"use step";\s*$', src, re.M):
        if not any(a <= m.start() <= b for a, b in spans):
            missed.append(src[:m.start()].count("\n")+1)
    print(f, "unwrapped:", missed[:12] if missed else "NONE")
