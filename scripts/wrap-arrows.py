import re
import pathlib

def find_body_end(src, open_pos):
    d = 0
    j = open_pos
    n = len(src)
    stack = []
    instr = None
    while j < n:
        c = src[j]
        if instr:
            if c == "\\":
                j += 2
                continue
            if instr == "`" and c == "$" and j + 1 < n and src[j + 1] == "{":
                stack.append(True)
                instr = None
                j += 2
                continue
            if c == instr:
                instr = None
            j += 1
            continue
        if c in "\"`":
            instr = c
            j += 1
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "/":
            k = src.find("\n", j)
            j = n if k < 0 else k
            continue
        if c == "/" and j + 1 < n and src[j + 1] == "*":
            e = src.find("*/", j)
            j = n if e < 0 else e + 2
            continue
        if c == "}" and stack and stack[-1]:
            stack.pop()
            instr = "`"
            j += 1
            continue
        if c == "{":
            d += 1
        elif c == "}":
            d -= 1
            if d == 0:
                return j
        j += 1
    return -1

p = pathlib.Path("/tmp/entry/apps/web/app/workflows/chat.ts")
s = p.read_text()

m = re.search(
    r'const convertMessages = async \(\n  messages: WebAgentUIMessage\[\],\n\): Promise<ModelMessage\[\]> => \{\n  "use step";\n',
    s,
)
assert m, "convertMessages"
s = s[: m.start()] + (
    "const convertMessages = makeStep(async function convertMessages(\n"
    "  messages: WebAgentUIMessage[],\n"
    "): Promise<ModelMessage[]> {\n"
) + s[m.end():]

m = re.search(r'const runAgentStep = async \((.*?)\)\s*=>\s*\{\n  "use step";\n', s, re.S)
assert m, "runAgentStep"
args = m.group(1)
s = s[: m.start()] + f"const runAgentStep = makeStep(async function runAgentStep({args}) {{\n" + s[m.end():]

for name in ("convertMessages", "runAgentStep"):
    m3 = re.search(rf"const {name} = makeStep\(async function {name}\(", s)
    open_pos = s.index("{", m3.end())
    end = find_body_end(s, open_pos)
    s = s[:end] + "});" + s[end + 1:]

p.write_text(s)
print("arrows wrapped OK")
