#!/usr/bin/env python3
"""
Backend del DE Challenges Hub.
- Sirve los estáticos (web/, catalog/, glossary.json) desde la raíz del proyecto.
- Expone POST /api/explain → llama a Groq con el token server-side (NUNCA en el frontend).

La key se lee de .env (GROQ_API_KEY=...) o de la variable de entorno. El .env está chmod 600
y en .gitignore; el frontend solo ve la respuesta de texto.

Uso:  python3 server.py [puerto]   (default 8765)
"""
import http.server, socketserver, json, os, sys, time, threading, urllib.request, urllib.error

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

def load_env():
    p = os.path.join(ROOT, ".env")
    if os.path.exists(p):
        for line in open(p):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()  # .env tiene prioridad sobre el entorno heredado
load_env()
GROQ_KEY = os.environ.get("GROQ_API_KEY", "")

# --- rate limiting (por IP + tope global diario), in-memory ---
RL_LOCK = threading.Lock()
RL = {}  # key -> [window_start, count]
RL_PER_IP   = int(os.environ.get("RL_PER_IP", "15"))      # consultas por IP
RL_WIN      = int(os.environ.get("RL_WIN", "600"))         # ventana por IP (s) = 10 min
RL_GLOBAL   = int(os.environ.get("RL_GLOBAL", "600"))      # tope global
RL_GLOBAL_WIN = 86400                                       # ventana global (s) = 1 día

def rate_check(ip):
    """Devuelve (ok, retry_after_segundos, scope)."""
    now = time.time()
    with RL_LOCK:
        for key, maxn, win in ((f"ip:{ip}", RL_PER_IP, RL_WIN), ("global", RL_GLOBAL, RL_GLOBAL_WIN)):
            ws, cnt = RL.get(key, [now, 0])
            if now - ws > win:
                ws, cnt = now, 0
            cnt += 1
            RL[key] = [ws, cnt]
            if cnt > maxn:
                return False, int(win - (now - ws)), key
    return True, 0, None

SYSTEM = (
    "Sos un tutor de Data Engineering y Databricks. Explicás términos y conceptos técnicos "
    "a personas junior que están armando su portafolio resolviendo pruebas técnicas reales. "
    "Respondé SIEMPRE en español, claro y conciso (máximo ~80 palabras). Interpretá el término "
    "en el contexto de Databricks, Spark, Delta Lake y pipelines de datos. Si el texto seleccionado "
    "es una dependencia, comando o fragmento de código, explicá qué hace. No inventes: si no estás "
    "seguro, decilo brevemente."
)

def groq_explain(term, ctx, item):
    if not GROQ_KEY:
        return None, "No hay GROQ_API_KEY configurada en el backend."
    item = item or {}
    ctx_txt = (
        f"Proyecto: catálogo de pruebas técnicas Databricks para Data Engineers junior.\n"
        f"Caso abierto: {item.get('title','(n/d)')}\n"
        f"Stack del caso: {', '.join(item.get('stack', []))}\n"
        f"Descripción del caso: {item.get('preview','')}\n"
        f"Texto alrededor de la selección: {ctx or '(n/d)'}\n"
    )
    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.3,
        "max_tokens": 240,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": f"{ctx_txt}\nExplicá este término/concepto seleccionado: \"{term}\""},
        ],
    }
    req = urllib.request.Request(
        GROQ_URL,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {GROQ_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "curl/8.4.0",  # evita el 403 de Cloudflare al UA por defecto de urllib
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.load(r)
        return d["choices"][0]["message"]["content"].strip(), None
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode()[:200]
        except Exception: pass
        return None, f"Groq HTTP {e.code} {body}"
    except Exception as e:
        return None, f"Error: {e}"

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=ROOT, **k)

    def _json(self, code, obj, extra=None):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path.split("?")[0] != "/api/explain":
            return self._json(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return self._json(400, {"error": "bad json"})
        term = (data.get("term") or "").strip()[:120]
        if not term:
            return self._json(400, {"error": "term vacío"})
        ip = (self.headers.get("X-Forwarded-For", "").split(",")[0].strip()
              or self.client_address[0])
        ok, retry, scope = rate_check(ip)
        if not ok:
            return self._json(429, {"error": "rate_limited", "retryAfter": retry, "scope": scope},
                              extra={"Retry-After": str(retry)})
        text, err = groq_explain(term, (data.get("context") or "")[:1200], data.get("item"))
        if err:
            return self._json(502, {"error": err})
        return self._json(200, {"term": term, "explanation": text, "model": GROQ_MODEL})

    def end_headers(self):
        # evitar caché del html mientras iteramos
        if self.path.endswith(".html") or self.path.endswith("/"):
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        super().end_headers()

    def log_message(self, *a):
        pass  # silencioso

if __name__ == "__main__":
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("", PORT), Handler) as httpd:
        status = "con Groq ✓" if GROQ_KEY else "SIN key ✗"
        print(f"DE Challenges Hub en http://localhost:{PORT}/web/  (LLM {status}, modelo {GROQ_MODEL})")
        httpd.serve_forever()
