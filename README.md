# Databricks Challenges Hub

Catálogo interactivo de **368 pruebas técnicas y ejercicios** de Databricks (Data Engineering, Data Analytics y ML) pensado para que personas junior armen su portafolio resolviendo material real y no demos cortas. Incluye un glosario de 50 términos y un "foquito" opcional que explica con un LLM cualquier texto seleccionado.

Versión web pública: https://analizodatos.com/data-challenges-hub

> Este repositorio contiene el **catálogo (datos), el motor web y los scripts**. No contiene el código de los repos catalogados: cada ítem enlaza al archivo exacto en su repo original.

## Qué contiene

- **368 ítems** en `catalog/items.json`: 254 ejercicios, 64 proyectos, 44 referencias de preguntas y respuestas, 5 sets de preguntas de entrevista y 1 take-home.
- **62 ítems end-to-end** (proyecto completo) y **306 micro** (ejercicio atómico). Dificultad: 91 beginner, 180 intermediate, 98 advanced.
- Cada ítem trae requisitos, stack, cloud, rubro, qué evalúa, si tiene enunciado y solución, si se autovalida, tiempo estimado y un **deep-link** a la fuente. Los end-to-end traen además un diagrama Mermaid.
- Etiquetas de **rol** (DE / DA / ML) y **plataforma** (Databricks / Fabric / agnóstico).
- Web con filtros multi-select, modo "Priorizar" (ranking por cuántos tags cumple cada ítem) frente a "Filtrar estricto", tema claro/oscuro, búsqueda de texto y panel de detalle.
- `lab/`: dos ejercicios propios con caso ficticio (Tambo Andino): medallion batch y streaming con DLT, en Markdown y PDF.

## Cómo se construyó el catálogo

1. **Fan-out de agentes en paralelo.** Los repos fuente se agruparon por afinidad (ejercicios de práctica, bootcamps, soluciones por industria, ML/GenAI, Fabric, preguntas de entrevista, etc.). Para cada grupo se lanzó un agente que leyó los repos y escribió su propio JSON parcial en `catalog/parts/<grupo>.json`, siguiendo el contrato de `catalog/SCHEMA.md`. Hoy hay 26 partes.
2. **Merge determinista.** `scripts/build_catalog.py` no usa ningún modelo: junta las partes, valida campos requeridos, enums (`scope`, `difficulty`) y que `source_url` sea una URL, y **aborta sin escribir nada** si hay duplicados de `id` o errores. Después normaliza `rubro` a un vocabulario controlado (reglas por palabras clave), y asigna `rol` (`scripts/classify_roles.py`) y `plataforma` por señales de texto.
3. **Resultado reproducible.** Con las mismas partes, el script produce exactamente el mismo `items.json` (verificado: salida idéntica byte a byte al regenerarlo).

```
catalog/parts/*.json  ->  validar + dedupe por id  ->  normalizar rubro  ->  rol  ->  plataforma  ->  catalog/items.json
```

## Estructura

```
catalog/
  SCHEMA.md        # contrato de cada ítem (campos y enums)
  parts/           # 1 JSON por grupo de repos (salida de cada agente)
  items.json       # catálogo mergeado: lo que consume la web
scripts/
  build_catalog.py # merge + validación + normalización (determinista)
  classify_roles.py# asigna rol DE/DA/ML por palabras clave
  build_deploy.sh  # arma una carpeta autocontenida para hosting Apache+PHP
  explain.php      # equivalente en PHP de /api/explain
web/
  index.html       # motor web (HTML + JS sin build)
  glossary.json    # glosario
lab/               # ejercicios propios (caso ficticio Tambo Andino)
server.py          # servidor local: estáticos + POST /api/explain
```

## Correr localmente

Solo la web (sin LLM; el foquito cae a enlaces a docs y búsqueda):

```bash
python3 -m http.server 8765
# abrir http://localhost:8765/web/
```

Con el foquito LLM:

```bash
cp .env.example .env      # completar GROQ_API_KEY (cuenta propia en Groq)
python3 server.py 8765
# abrir http://localhost:8765/web/
```

Regenerar el catálogo tras editar partes: `python3 scripts/build_catalog.py`.

## Endpoint `/api/explain`

`POST /api/explain` con JSON `{"term": "...", "context": "...", "item": {...}}` devuelve `{"term", "explanation", "model"}`. Llama a la API de Groq (modelo por defecto `llama-3.1-8b-instant`, configurable con `GROQ_MODEL`) con la clave **solo del lado servidor**: variable de entorno o `.env` en local, y variable de entorno o `api/groq.key` en el hosting PHP. El navegador nunca ve la clave. Incluye rate limit en memoria (15 consultas por IP cada 10 minutos y 600 globales por día; ajustable con `RL_PER_IP`, `RL_WIN`, `RL_GLOBAL`). **Este repo no incluye ninguna clave.**

## Límites honestos

- **Los campos descriptivos los generó un LLM leyendo los repos**: `preview`, `requirements`, `difficulty`, `est_minutes`, `portfolio_value`, `diagram`. La dificultad y los tiempos son estimaciones, no mediciones, y no se revisaron uno por uno a mano. `difficulty_source` indica si venía declarada en el repo o se infirió.
- `rol`, `plataforma` y `rubro` se asignan con **reglas de palabras clave**: hay falsos positivos y negativos (un ítem puede salir como DE+DA+ML sin serlo del todo).
- Los ítems apuntan a ramas vivas de repos de terceros (principalmente `main`): los enlaces pueden romperse o el contenido puede cambiar. No hay chequeo automático de enlaces.
- Los repos fuente **no se incluyen**; no se verificó que las soluciones o los notebooks de cada uno corran hoy. Cada repo conserva su propia licencia; el catálogo solo resume y enlaza.
- Parte de las fuentes no son repos (certificaciones, blogs, plataformas de práctica); en esos ítems el campo `repo` empieza con "—".
- Los sets de entrevista de empresas reales (por ejemplo Swiggy, Flipkart, PhonePe) y el take-home de PostNord vienen de repos públicos de terceros; se resumen, no se reproducen. Su veracidad es la de la fuente.
- El foquito depende de un tercero (Groq) y de un modelo pequeño: puede equivocarse.
- Sin pruebas automáticas más allá de la validación del merge.

## Licencia

MIT para el código y los datos de este repositorio (ver `LICENSE`). El material enlazado pertenece a sus autores.
