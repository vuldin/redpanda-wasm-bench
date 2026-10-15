#!/usr/bin/env bash
# Deploy a wasm transform through the admin API rather than rpk.
#
# Why not `rpk transform deploy`: the cluster's rpk is the stock package, and
# only the admin API's deploy body carries `state_options`
# (require_state_recovery). It also does the one thing a stateful guest needs
# before deploy: trust the binary's sha256 with the shared_memory capability.
# Without that grant the broker never links redpanda_wasm_shared_memory and a
# guest importing it does not instantiate at all - for STATE=off too.
#
# The trust step is a read-merge of wasm_trusted_modules, so other entries
# (and other capabilities on this sha) survive.
#
# ADMIN_URL may be a comma-separated list. Deploy and config writes are
# retried across all of them with backoff: a broker that is not the leader of
# the partition the request needs answers 503 "Leader not available", which
# rpk retries elsewhere and so must this.
#
# usage: deploy-transform.sh ADMIN_URL[,ADMIN_URL...] NAME INPUT_TOPIC OUTPUT_TOPIC WASM \
#          [--require-state-recovery] [--from-start] [--env K=V]...
set -euo pipefail
admin="$1" name="$2" input="$3" output="$4" wasm="$5"
shift 5
require=false offset=""
envs=()
while [ $# -gt 0 ]; do
  case "$1" in
    --require-state-recovery) require=true ;;
    --from-start) offset=from_start ;;
    --env) envs+=("$2"); shift ;;
    *) echo "deploy-transform: unknown arg $1" >&2; exit 2 ;;
  esac
  shift
done
[ -f "$wasm" ] || { echo "deploy-transform: $wasm not found" >&2; exit 1; }

python3 -I - "$admin" "$name" "$input" "$output" "$wasm" "$require" "$offset" "${envs[@]+"${envs[@]}"}" <<'PY'
import hashlib, json, sys, time, urllib.request, urllib.error
admin, name, inp, out, wasm, require, offset, *envs = sys.argv[1:]
data = open(wasm, "rb").read()
sha = hashlib.sha256(data).hexdigest()

admins = [a.strip() for a in admin.split(",") if a.strip()]

def req_once(base, method, path, body, ctype, hops=0):
    url = base + path if path.startswith("/") else path
    r = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        # The admin API redirects writes to the controller leader with 307/308.
        # urllib will not re-send a POST/PUT body across a redirect, so follow
        # it here. (2026-10-15: a deploy answered 307 was treated as final and
        # a whole e2 phase failed.)
        loc = e.headers.get("Location") if e.headers else None
        if e.code in (301, 302, 307, 308) and loc and hops < 3:
            return req_once(base, method, loc, body, ctype, hops + 1)
        return e.code, e.read()
    except Exception as e:  # connection refused, timeout: try the next broker
        return 599, str(e).encode()

def req(method, path, body=None, ctype="application/json"):
    st, out = 599, b"no admin endpoint"
    for attempt in range(8):
        for base in admins:
            st, out = req_once(base, method, path, body, ctype)
            if st < 500:
                return st, out
        time.sleep(min(2 * (attempt + 1), 8))
    return st, out

st, body = req("GET", "/v1/cluster_config")
if st != 200:
    sys.exit(f"deploy-transform: GET cluster_config -> {st} {body[:300]!r}")
cfg = json.loads(body)
mods = cfg.get("wasm_trusted_modules") or []
merged, found = [], False
for m in mods:
    if m.get("sha256") == sha:
        found = True
        caps = sorted(set(m.get("capabilities", [])) | {"shared_memory"})
        m = dict(m, capabilities=caps)
    merged.append(m)
if not found:
    merged.append({"sha256": sha, "capabilities": ["shared_memory"]})
if merged != mods:
    st, body = req("PUT", "/v1/cluster_config", json.dumps({"upsert": {"wasm_trusted_modules": merged}, "remove": []}).encode())
    if st != 200:
        sys.exit(f"deploy-transform: trusting {sha[:12]} failed -> {st} {body[:300]!r}")
    # Config propagates asynchronously to every shard; wait until it reads back.
    for _ in range(30):
        st, body = req("GET", "/v1/cluster_config")
        if st == 200 and any(m.get("sha256") == sha and "shared_memory" in m.get("capabilities", [])
                             for m in json.loads(body).get("wasm_trusted_modules") or []):
            break
        time.sleep(1)
    else:
        sys.exit("deploy-transform: trusted-module grant never read back")
    time.sleep(2)
print(f"deploy-transform: {sha[:12]} trusted (shared_memory)")

doc = {"name": name, "input_topic": inp, "output_topics": [out],
       "environment": [{"key": k, "value": v} for k, v in (e.split("=", 1) for e in envs)]}
if require == "true":
    doc["state_options"] = {"require_state_recovery": True}
if offset:
    doc["offset"] = {"format": offset, "value": 0}
st, body = req("POST", "/v1/transform/deploy", json.dumps(doc).encode() + data, "application/octet-stream")
if st != 200:
    sys.exit(f"deploy-transform: deploy {name} -> {st} {body[:500]!r}")
print(f"deploy-transform: deployed {name} sha256={sha} require_state_recovery={require} offset={offset or 'default'} env={envs}")
PY
