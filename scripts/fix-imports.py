import pathlib

imp = 'import { makeStep } from "lightflow-engine/compat/workflow";'
for f in pathlib.Path("/tmp/entry/apps/web/app/workflows").glob("*.ts"):
    s = f.read_text()
    lines = s.split("\n")
    had = any(ln.strip() == imp for ln in lines)
    lines = [ln for ln in lines if ln.strip() != imp]
    if had:
        for idx, ln in enumerate(lines[:60]):
            if ln.startswith("import "):
                lines.insert(idx, imp)
                break
    f.write_text("\n".join(lines))
    print(f.name, "dedup:", had)
