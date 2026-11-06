"""Column-projected extract of a large Stack Exchange XML file (run on the source machine).

Drops free text (post bodies, comment text, edit text) and keeps only the listed attributes,
one TSV line per <row>. Resumable: each run processes ~33 s of input from the saved byte offset,
so it fits inside a time-limited shell. Dump files have exactly one <row .../> per line.

Usage: python3 compact_extract.py <path/to/Posts.xml> posts Id,PostTypeId,...  (repeat until done)
"""
import re, sys, os, time, gzip, json
src, name, fields = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
state_f = os.path.expanduser(f"~/compact_{name}.state")
st = json.load(open(state_f)) if os.path.exists(state_f) else {"off": 0, "rows": 0, "part": 0, "done": False}
if st["done"]: print("ALREADY DONE", st); sys.exit()
# \b prevents Id= matching ParentId= / OwnerUserId= etc.; attribute values in the dump are XML-escaped.
pat = {f: re.compile(r'\b' + f + r'="([^"]*)"') for f in fields}
t0 = time.time(); out = []
with open(src, "rb") as fh:
    fh.seek(st["off"])
    while time.time() - t0 < 33:
        line = fh.readline()
        if not line: st["done"] = True; break
        if b"<row " not in line: continue
        s = line.decode("utf-8", "replace")
        out.append("\t".join((m.group(1) if (m := pat[f].search(s)) else "") for f in fields))
    st["off"] = fh.tell()
os.makedirs(os.path.expanduser(f"~/compact/{name}"), exist_ok=True)
if out:
    with gzip.open(os.path.expanduser(f"~/compact/{name}/part{st['part']:04d}.tsv.gz"), "wt", compresslevel=3) as g:
        g.write("\n".join(out) + "\n")
    st["part"] += 1; st["rows"] += len(out)
json.dump(st, open(state_f, "w")); print(name, st)
