<?php
/**
 * Proxy del LLM (Groq) para el DE/DA Challenges Hub — equivalente a /api/explain de server.py.
 * El token vive en el servidor (api/groq.key protegido por .htaccess, o env GROQ_API_KEY).
 * El frontend NUNCA ve la key: solo recibe el texto explicado.
 */
header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405); echo json_encode(['error' => 'method not allowed']); exit;
}

// 1) key: variable de entorno o archivo protegido junto a este .php
$key = getenv('GROQ_API_KEY');
if (!$key) { $f = __DIR__ . '/groq.key'; if (is_readable($f)) $key = trim(file_get_contents($f)); }
if (!$key) { http_response_code(500); echo json_encode(['error' => 'GROQ_API_KEY no configurada']); exit; }

$model = getenv('GROQ_MODEL') ?: 'llama-3.1-8b-instant';

// 2) rate limiting (por IP + tope global diario) — file-based, sin DB
$RL_PER_IP     = (int)(getenv('RL_PER_IP')   ?: 15);    // consultas por IP
$RL_WIN        = (int)(getenv('RL_WIN')      ?: 600);   // ventana por IP (s) = 10 min
$RL_GLOBAL     = (int)(getenv('RL_GLOBAL')   ?: 600);   // tope global
$RL_GLOBAL_WIN = 86400;                                  // ventana global (s) = 1 día

function client_ip() {
    $xff = $_SERVER['HTTP_X_FORWARDED_FOR'] ?? '';
    if ($xff) { $p = explode(',', $xff); return trim($p[0]); }
    return $_SERVER['REMOTE_ADDR'] ?? '0.0.0.0';
}
/** fixed-window: incrementa y devuelve [ok, retry_after] */
function rl_hit($dir, $key, $max, $win) {
    if (!is_dir($dir)) @mkdir($dir, 0700, true);
    $f = $dir . '/' . preg_replace('/[^a-z0-9_]/', '', $key) . '.json';
    $fp = @fopen($f, 'c+');
    if (!$fp) return [true, 0]; // si no se puede escribir, no bloquear
    flock($fp, LOCK_EX);
    $raw = stream_get_contents($fp);
    $st  = json_decode($raw, true) ?: ['ws' => time(), 'c' => 0];
    $now = time();
    if ($now - $st['ws'] > $win) { $st = ['ws' => $now, 'c' => 0]; }
    $st['c']++;
    ftruncate($fp, 0); rewind($fp); fwrite($fp, json_encode($st));
    flock($fp, LOCK_UN); fclose($fp);
    if ($st['c'] > $max) return [false, (int)($win - ($now - $st['ws']))];
    return [true, 0];
}
$rlDir = __DIR__ . '/ratelimit';
// GC probabilístico (1%): borrar archivos viejos
if (mt_rand(1, 100) === 1 && is_dir($rlDir)) {
    foreach (glob($rlDir . '/*.json') as $g) { if (time() - filemtime($g) > 86400) @unlink($g); }
}
list($okIp, $retIp)         = rl_hit($rlDir, 'ip_' . md5(client_ip()), $RL_PER_IP, $RL_WIN);
list($okGlobal, $retGlobal) = rl_hit($rlDir, 'global', $RL_GLOBAL, $RL_GLOBAL_WIN);
if (!$okIp || !$okGlobal) {
    $retry = !$okIp ? $retIp : $retGlobal;
    http_response_code(429);
    header('Retry-After: ' . $retry);
    echo json_encode(['error' => 'rate_limited', 'retryAfter' => $retry,
                      'scope' => !$okIp ? 'ip' : 'global']);
    exit;
}

$body = json_decode(file_get_contents('php://input'), true) ?: [];
$term = trim((string)($body['term'] ?? ''));
if ($term === '') { http_response_code(400); echo json_encode(['error' => 'term vacío']); exit; }
$term = mb_substr($term, 0, 120);
$ctx  = mb_substr((string)($body['context'] ?? ''), 0, 1200);
$item = is_array($body['item'] ?? null) ? $body['item'] : [];
$title   = $item['title'] ?? '(n/d)';
$stack   = isset($item['stack']) && is_array($item['stack']) ? implode(', ', $item['stack']) : '';
$preview = $item['preview'] ?? '';

$system = "Sos un tutor de Data Engineering / Analytics y Databricks. Explicás términos y conceptos técnicos "
        . "a personas junior que arman su portafolio resolviendo pruebas técnicas reales. Respondé SIEMPRE en "
        . "español, claro y conciso (máximo ~80 palabras). Interpretá el término en el contexto de Databricks, "
        . "Spark, Delta Lake, SQL y BI. Si el texto es una dependencia/comando/código, explicá qué hace. "
        . "No inventes: si no estás seguro, decilo brevemente.";
$user = "Proyecto: catálogo de pruebas técnicas Databricks para Data Engineers/Analysts junior.\n"
      . "Caso abierto: $title\nStack del caso: $stack\nDescripción del caso: $preview\n"
      . "Texto alrededor de la selección: $ctx\n\nExplicá este término/concepto seleccionado: \"$term\"";

$payload = json_encode([
    'model' => $model, 'temperature' => 0.3, 'max_tokens' => 240,
    'messages' => [
        ['role' => 'system', 'content' => $system],
        ['role' => 'user',   'content' => $user],
    ],
]);

$ch = curl_init('https://api.groq.com/openai/v1/chat/completions');
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST           => true,
    CURLOPT_POSTFIELDS     => $payload,
    CURLOPT_HTTPHEADER     => [
        'Authorization: Bearer ' . $key,
        'Content-Type: application/json',
        'User-Agent: curl/8.4.0',   // evita el 403 de Cloudflare al UA por defecto
        'Accept: application/json',
    ],
    CURLOPT_TIMEOUT => 30,
]);
$resp = curl_exec($ch);
if ($resp === false) { http_response_code(502); echo json_encode(['error' => 'curl: ' . curl_error($ch)]); curl_close($ch); exit; }
$code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);
if ($code !== 200) { http_response_code(502); echo json_encode(['error' => "Groq HTTP $code"]); exit; }

$d = json_decode($resp, true);
$text = trim($d['choices'][0]['message']['content'] ?? '');
echo json_encode(['term' => $term, 'explanation' => $text, 'model' => $model], JSON_UNESCAPED_UNICODE);
