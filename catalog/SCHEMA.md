# Esquema del catálogo — databricks-challenges-hub

Cada ítem del catálogo (`items.json`) representa **una unidad accionable**: un ejercicio atómico, un proyecto end-to-end, un set de preguntas de entrevista de una empresa, o un take-home real. Granularidad variable según el material (un repo puede aportar 1 proyecto o N ejercicios).

## Campos

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | string | Slug único: `{repo}__{seccion}__{tarea}` |
| `title` | string | Nombre legible de la unidad |
| `repo` | string | `owner/repo` de GitHub |
| `repo_local` | string | Ruta local relativa (`repos/...`) |
| `source_url` | string | URL pública |
| `path` | string | Ruta interna al archivo/sección dentro del repo |
| `type` | enum | `exercise` · `project` · `take-home` · `interview-questions` · `qa-reference` |
| `scope` | enum | **`micro`** (mini-tarea / ejercicio atómico) · **`end-to-end`** (proyecto completo ingest→gold) — eje principal para distinguir building-blocks de proyectos de portafolio |
| `requirements` | string[] | Bullets de qué pide la tarea (se muestran en el panel de detalle) |
| `diagram` | string\|null | Diagrama de flujo en sintaxis Mermaid (sobre todo para `end-to-end`); `null` si no amerita |
| `difficulty` | enum | **`beginner` · `intermediate` · `advanced`** — mi evaluación leyendo la tarea |
| `difficulty_source` | enum | `declared` (lo dice el repo) · `inferred` (lo deduje yo) · `declared+verified` (lo dice y lo confirmé leyendo) |
| `stack` | string[] | Tecnologías: Databricks, Delta Lake, DLT, Spark SQL, PySpark, Unity Catalog, Auto Loader, dbt, Power BI… |
| `cloud` | string[] | `Azure` · `AWS` · `GCP` · `Databricks-pure` · `Databricks Free Edition` · `local/OSS` |
| `rubro` | string[] | Industria/dominio: retail, e-commerce, food-delivery, logística, fintech/banca, telco, salud, IoT, genérico |
| `evaluates` | string[] | Skills: ingestion, ETL/ELT, streaming, MERGE/upsert, SCD2, window functions, optimización, modelado dimensional, data quality, schema evolution… |
| `has_statement` | bool | ¿Trae enunciado/requisitos claros? |
| `has_solution` | bool | ¿Trae solución de referencia? |
| `solution_loc` | string\|null | Dónde vive la solución |
| `self_validating` | bool | ¿Tiene asserts/tests pass-fail automáticos? |
| `runnable` | enum | `databricks` (necesita workspace, Free Edition OK) · `databricks+azure` (necesita ADLS/SP) · `local` (Spark/PySpark local) |
| `est_minutes` | number\|null | Tiempo estimado |
| `preview` | string | Resumen del enunciado (1-3 frases) para el card del buscador |
| `portfolio_value` | string | Cómo usarlo en un portafolio (proyecto standalone vs building-block vs práctica) |
| `notes` | string | Caveats, gotchas |

## Mapeo seniority ↔ dificultad (acordado)
`beginner` ≈ junior · `intermediate` ≈ semi-senior · `advanced` ≈ senior. Se usa dificultad como eje primario (más fiel; casi ningún repo etiqueta seniority formal).
