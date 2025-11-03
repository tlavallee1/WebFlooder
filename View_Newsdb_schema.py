import sqlite3
from textwrap import indent

DB = "news.db"

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
cur = con.cursor()

def rows(q, *p):
    cur.execute(q, p)
    return cur.fetchall()

print(f"\n=== DATABASE: {DB} ===")

# Full CREATE statements (tables, indexes, views, triggers)
print("\n--- sqlite_master (CREATE statements) ---")
for r in rows("""SELECT type, name, tbl_name, sql
                 FROM sqlite_master
                 WHERE sql IS NOT NULL
                 ORDER BY CASE type
                            WHEN 'table' THEN 1
                            WHEN 'index' THEN 2
                            WHEN 'view'  THEN 3
                            WHEN 'trigger' THEN 4
                            ELSE 5
                          END, name"""):
    print(f"\n[{r['type'].upper()}] {r['name']} (on {r['tbl_name']})")
    print(indent(r["sql"], "    "))

# Table-by-table column details + indexes + FKs
tables = [r["name"] for r in rows(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
)]

for t in tables:
    print(f"\n--- TABLE: {t} ---")
    cols = rows(f"PRAGMA table_info({t})")
    if cols:
        print("Columns:")
        for c in cols:
            pk = " PK" if c["pk"] else ""
            notnull = " NOT NULL" if c["notnull"] else ""
            dflt = f" DEFAULT {c['dflt_value']}" if c["dflt_value"] is not None else ""
            print(f"  - {c['name']} {c['type']}{notnull}{dflt}{pk}")
    else:
        print("  (no columns found)")

    # Indexes
    idxs = rows(f"PRAGMA index_list({t})")
    if idxs:
        print("Indexes:")
        for i in idxs:
            iname = i["name"]
            unique = " UNIQUE" if i["unique"] else ""
            print(f"  - {iname}{unique}")
            info = rows(f"PRAGMA index_info({iname})")
            cols = ", ".join(ii["name"] for ii in info) if info else "(expr/partial)"
            print(f"      columns: {cols}")
    # Foreign keys
    fks = rows(f"PRAGMA foreign_key_list({t})")
    if fks:
        print("Foreign keys:")
        for fk in fks:
            print(f"  - ({fk['from']}) → {fk['table']}({fk['to']}) on update {fk['on_update']} on delete {fk['on_delete']}")

# Views (optional: expanded)
views = rows("SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name")
if views:
    print("\n--- VIEWS ---")
    for v in views:
        print(f"\n[VIEW] {v['name']}\n{indent(v['sql'], '    ')}")

con.close()
print("\n=== END ===\n")
