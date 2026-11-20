#!/usr/bin/env python3
"""
Pipeline ÚNICO y re-ejecutable del catálogo:
  catalog/parts/*.json  →  merge + validar  →  normalizar rubro (+domain_detail)  →  clasificar rol  →  catalog/items.json

Correr cada vez que se agregan/editan partes. Idempotente.
"""
import json, glob, os, collections, importlib.util
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARTS = os.path.join(ROOT, "catalog", "parts")
OUT = os.path.join(ROOT, "catalog", "items.json")

# --- reusar la lógica de roles ---
spec = importlib.util.spec_from_file_location("cr", os.path.join(ROOT, "scripts", "classify_roles.py"))
cr = importlib.util.module_from_spec(spec); spec.loader.exec_module(cr)

REQ = ["id","title","repo","source_url","type","scope","difficulty","stack","cloud","rubro","evaluates","has_statement","has_solution","preview"]
SCOPES = {"micro","end-to-end"}; DIFFS = {"beginner","intermediate","advanced"}
LIST_FIELDS = ["stack","cloud","rubro","evaluates","requirements"]

# --- normalización de rubro a vocabulario controlado ---
RULES = [
  ("logística",      ["postal","última milla","ultima milla","last-mile","logíst","logist","supply","routing","rutas","mobility","ride-hail","viajes/rutas"]),
  ("food-delivery",  ["food-delivery","food/restaur","food ","restaurant","delivery"]),
  ("travel",         ["travel","viaje","turismo"]),
  ("finanzas/banca", ["fintech","banca","banc","pagos","pago","aml","finanz","seguros","value-at-risk","capm","payment"]),
  ("salud",          ["salud","health","omop","patolog","dicom","oncolog","drug","clinical"]),
  ("manufactura/IoT",["manufactura","iot","sensor","factory","digital twin","digital-twin","edge","readings"]),
  ("telco",          ["telco","telecom"]),
  ("marketing/adtech",["marketing","adtech","attribution","media-mix","media mix","bidding","propensity","segment"]),
  ("gaming",         ["gaming","gamer"]),
  ("ciberseguridad", ["ciberseg","cyber","dns","threat","security","pii","privacidad"]),
  ("e-commerce",     ["e-commerce","ecommerce","clickstream","events","web","usuario"]),
  ("retail",         ["retail","ventas","loyalty","pos ","fmcg","order","product","customer","transaction","sales","merch"]),
]
GENERIC_MARK = ["genérico","generico","agnóstico","agnostico","data engineering","transformation","ingestion",
  "modeling","orchestration","lakehouse","platform","performance","etl","data-quality","data-modeling",
  "certificacion","certificación","general","educación","educacion","infraestructura","países","paises",
  "roads","traffic","empleados","api/","fundament","cross-industry","data warehous","analytics","bi ","sql"]

# --- clasificación de plataforma (Databricks / Fabric / agnóstico) ---
DBX_MARK = ["databricks","delta lake","delta live"," dlt","dlt ","unity catalog","auto loader","autoloader",
  "mosaic ai","mosaic","genie","asset bundle","lakehouse monitoring","free edition","z-order","zorder",
  "liquid clustering","apply_changes","create_auto_cdc","cloudfiles","lakeflow","metric view","trusted asset",
  "spark declarative","databricks sql","tvf","table-valued function","dbutils","scd type 2 via","feature serving"]
FABRIC_MARK = ["microsoft fabric"," fabric","onelake","directlake","direct lake","dataflows gen2","kql ",
  "eventstream","data activator","warehouse fabric","fabric lakehouse"]

def canon_plataforma(it):
    h = hay(it)  # definida abajo via cr.hay
    if any(k in h for k in FABRIC_MARK): return ["Fabric"]
    if any(k in h for k in DBX_MARK): return ["Databricks"]
    return ["agnóstico"]

def hay(it):
    parts = [it.get("type",""), it.get("scope",""), it.get("title",""), it.get("preview",""), it.get("notes","")]
    parts += it.get("stack",[]) + it.get("evaluates",[]) + it.get("requirements",[]) + it.get("rubro",[])
    return " ".join(str(p) for p in parts).lower()

def canon_rubro(orig_list):
    s = " ; ".join(orig_list).lower()
    tags = [name for name, kws in RULES if any(k in s for k in kws)]
    has_generic = any(g in s for g in GENERIC_MARK)
    if has_generic and (not tags or set(tags) <= {"retail","e-commerce"}):
        return ["genérico/fundamentos"]
    if not tags:
        return ["genérico/fundamentos"]
    out = []
    for t in tags:
        if t not in out: out.append(t)
    return out[:2]

def main():
    parts = sorted(glob.glob(os.path.join(PARTS, "*.json")))
    items = []; problems = []
    for p in parts:
        try: d = json.load(open(p))
        except Exception as e: problems.append(f"{os.path.basename(p)}: JSON inválido {e}"); continue
        if not isinstance(d, list): problems.append(f"{os.path.basename(p)}: no es array"); continue
        for it in d:
            miss = [k for k in REQ if k not in it]
            if miss: problems.append(f"{it.get('id','?')} ({os.path.basename(p)}): faltan {miss}")
            if it.get("scope") not in SCOPES: problems.append(f"{it.get('id')}: scope '{it.get('scope')}'")
            if it.get("difficulty") not in DIFFS: problems.append(f"{it.get('id')}: difficulty '{it.get('difficulty')}'")
            if not str(it.get("source_url","")).startswith("http"): problems.append(f"{it.get('id')}: source_url")
        items += d
        print(f"  {os.path.basename(p):40} {len(d):>4}")

    ids = [it["id"] for it in items if "id" in it]
    dups = [i for i, c in collections.Counter(ids).items() if c > 1]
    print(f"\nTOTAL: {len(items)} | problemas: {len(problems)} | dups: {dups or 'ninguno'}")
    for pr in problems[:25]: print("  -", pr)
    if dups or problems:
        print("\n!! No escribo items.json hasta resolver lo anterior"); return

    for it in items:
        # normalizar campos de lista
        for f in LIST_FIELDS:
            v = it.get(f)
            it[f] = [v] if isinstance(v, str) else (v or [])
        # rubro -> vocab controlado + detalle
        orig = it["rubro"]
        it["domain_detail"] = " ; ".join(orig) if orig else ""
        it["rubro"] = canon_rubro(orig)
        # rol
        it["rol"] = cr.classify(it)
        # plataforma (Databricks / Fabric / agnóstico)
        it["plataforma"] = canon_plataforma(it)

    json.dump(items, open(OUT, "w"), ensure_ascii=False, indent=2)
    from collections import Counter
    print(f"\n-> items.json ESCRITO: {len(items)}")
    print("rubro:", dict(Counter(r for i in items for r in i["rubro"]).most_common()))
    print("rol:", dict(Counter(r for i in items for r in i["rol"]).most_common()))
    print("plataforma:", dict(Counter(p for i in items for p in i["plataforma"]).most_common()))
    print("scope:", dict(Counter(i["scope"] for i in items)))
    print("dificultad:", dict(Counter(i["difficulty"] for i in items)))

if __name__ == "__main__":
    main()
