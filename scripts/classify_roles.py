#!/usr/bin/env python3
"""
Asigna el tag `rol` (lista: DE / DA / ML) a cada ítem de catalog/items.json.
Un ítem puede tener varios roles (ej. fundamentos SQL = DE+DA = "compartido").
Determinístico, basado en señales de type/scope/stack/evaluates/title/preview.
Re-ejecutable: idempotente (sobrescribe `rol`).
"""
import json, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ITEMS = os.path.join(ROOT, "catalog", "items.json")

# Señales por rol (substring, lowercase)
DE_KW = ["pipeline","ingest","ingesta","etl","elt","streaming","dlt","delta live","auto loader",
  "autoloader","medallion","bronze","silver","asset bundle"," dab","orquest","orchestrat","workflow",
  "spark","partition","shuffle","skew","salting","optimize","z-order","zorder","vacuum",
  "liquid clustering","merge","upsert","scd","change data feed","checkpoint","watermark",
  "structured streaming","parquet","schema evolution","schema enforcement","time travel",
  "unity catalog","cdc","copy into","rescued data","stream-static","foreachbatch","catalyst","aqe"]

DA_KW = ["power bi","powerbi"," dax","genie","dashboard","reporting","report ","visualiz","business intelligence",
  "tableau","looker","star schema","dimensional","data mart","modelo estrella","rls","row-level","row level",
  "natural language","ai/bi","ai-bi","kpi","métricas de negocio","metricas de negocio","gold aggreg",
  "agregaciones gold","window function","window-functions","ventana","ranking","rolling","gap-and-island",
  "pivot","cohort","funnel","retention","sql analít","sql analit","analytics-ready","business-ready",
  "consulta sql","sql interview","sql question","storytelling","metric view","semantic layer","capa semántica",
  "capa semantica","trusted asset","table-valued","tvf","databricks sql","data warehouse","data warehousing",
  "eda","exploratory","cube","drill","slicer","measure","medida dax","insight"]

ML_KW = ["mlflow","machine learning","xgboost","pytorch","tensorflow","sklearn","scikit","spark nlp",
  " nlp","forecast","hyperopt","feature engineering","feature store","recommend","anomaly","deep learning",
  "neural","propensity","churn","monte carlo","graphframes","prophet","holt-winters","classifier",
  "k-means","kmeans","topic modeling","latent dirichlet","segmentation","model serving","model registry",
  "inception","vgg","random forest","collaborative filter","survival analysis","capm","value at risk","fraud",
  "mlops","vector search","retrieval augmented","retrieval-augmented","genai","gen ai","generative ai",
  "large language","llm","autoML","auto ml","lightgbm","model drift","data drift","lakehouse monitoring",
  "fine-tun","model deployment","model training","feature serving","mosaic ai","foundation model",
  "experiment tracking","hyperparameter","model serving","modelo de ml","entrenamiento de modelo","rag "]

def hay(it):
    parts = [it.get("type",""), it.get("scope",""), it.get("title",""), it.get("preview","")]
    parts += it.get("stack",[]) + it.get("evaluates",[]) + it.get("requirements",[])
    return " ".join(str(p) for p in parts).lower()

def classify(it):
    h = hay(it)
    roles = []
    if any(k in h for k in DE_KW): roles.append("DE")
    if any(k in h for k in DA_KW): roles.append("DA")
    if any(k in h for k in ML_KW): roles.append("ML")
    # qa-reference e interview-questions de SQL/modelado sirven a DA aunque no matchee
    if it.get("type") in ("qa-reference","interview-questions") and "DA" not in roles:
        if any(k in h for k in ["sql","modelad","modeling","dimensional","data model"]):
            roles.append("DA")
    # default: catálogo DE-céntrico
    if not roles: roles = ["DE"]
    # orden estable
    order = {"DE":0,"DA":1,"ML":2}
    return sorted(set(roles), key=lambda r: order[r])

def main():
    items = json.load(open(ITEMS))
    from collections import Counter
    c = Counter(); combos = Counter()
    for it in items:
        it["rol"] = classify(it)
        for r in it["rol"]: c[r]+=1
        combos["+".join(it["rol"])]+=1
    json.dump(items, open(ITEMS,"w"), ensure_ascii=False, indent=2)
    print("Roles (un ítem puede tener varios):")
    for r,n in c.most_common(): print(f"  {r:4} {n}")
    print("\nCombinaciones:")
    for k,n in combos.most_common(): print(f"  {k:12} {n}")
    print(f"\nTotal ítems: {len(items)}")

if __name__ == "__main__":
    main()
