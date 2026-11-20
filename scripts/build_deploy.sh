#!/usr/bin/env bash
# Genera deploy/ — carpeta autocontenida lista para subir a un hosting normal (cPanel/Apache+PHP).
# Estructura de salida:
#   index.html  ·  glossary.json  ·  catalog/items.json  ·  api/explain.php  ·  api/.htaccess
# La clave de Groq NO se incluye: definir GROQ_API_KEY como variable de entorno del hosting
# o crear manualmente api/groq.key en el servidor (nunca en el repo).
set -e
BASE="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$BASE/deploy"
rm -rf "$OUT"; mkdir -p "$OUT/catalog" "$OUT/api" "$OUT/api/ratelimit"

# index.html: paths relativos (subcarpeta) + endpoint PHP
sed -e 's#\.\./catalog/items.json#catalog/items.json#g' \
    -e 's#content="/api/explain"#content="api/explain.php"#' \
    "$BASE/web/index.html" > "$OUT/index.html"

cp "$BASE/web/glossary.json"   "$OUT/glossary.json"
cp "$BASE/catalog/items.json"  "$OUT/catalog/items.json"
cp "$BASE/scripts/explain.php" "$OUT/api/explain.php"

# .htaccess: bloquea el acceso web a la key y al store del rate-limit (PHP los lee por filesystem)
cat > "$OUT/api/.htaccess" <<'HT'
<FilesMatch "(groq\.key|\.json)$">
  Require all denied
</FilesMatch>
# Apache 2.2 fallback
<IfModule !mod_authz_core.c>
  <FilesMatch "(groq\.key|\.json)$">
    Order allow,deny
    Deny from all
  </FilesMatch>
</IfModule>
HT
# proteger también la carpeta ratelimit por las dudas
cat > "$OUT/api/ratelimit/.htaccess" <<'HT'
Require all denied
<IfModule !mod_authz_core.c>
  Order allow,deny
  Deny from all
</IfModule>
HT
touch "$OUT/api/ratelimit/.gitkeep"

echo "OK -> $OUT"
find "$OUT" -type f | sed "s#$OUT/#  #"
