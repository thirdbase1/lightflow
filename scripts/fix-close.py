import pathlib

p = pathlib.Path("/tmp/entry/apps/web/app/workflows/chat-sandbox-runtime.ts")
s = p.read_text()
idx = s.index("persistImageAttachmentsToSandbox = makeStep")
old = "  return paths;\n}\n"
j = s.index(old, idx)
s = s[:j] + "  return paths;\n});\n" + s[j + len(old):]
p.write_text(s)
print("closed")
