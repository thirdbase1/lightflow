import re
files = ["app/workflows/chat.ts","app/workflows/sandbox-lifecycle.ts","app/workflows/run-benchmarks.ts","app/workflows/chat-post-finish.ts"]
import subprocess
for f in files:
    s = open("/tmp/entry/apps/web/" + f).read()
    real = len(re.findall(r'^\s*"use step";\s*$', s, re.M))
    print(f, real, "remaining directives;", s.count("makeStep"), "makeStep refs")
