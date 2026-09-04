#!/usr/bin/env python3
"""Preflight: every file the playbook copies out of the staging dir must be there.

Two deploys have failed because a runner referenced a file the deploy never
staged: relay-sink.wasm, then run-round3.sh (2026-08-31, "Could not find or
access '/tmp/cloud-deploy/run-round3.sh' on the Ansible Controller"). Both cost
a full provision+deploy cycle on real AWS spend to discover.

The playbook's `src:` entries under {{ local_artifacts_dir }} ARE the contract
for what the staging dir must contain, so this checks that contract directly
instead of guessing from grep. Runs in milliseconds, before ansible starts.
"""
import sys, yaml

def expand(task):
    """Yield the artifact-relative paths one task copies out of the staging dir."""
    for key in ("copy", "template"):
        spec = task.get(key)
        if not isinstance(spec, dict):
            continue
        src = spec.get("src")
        if not isinstance(src, str) or "local_artifacts_dir" not in src:
            continue
        # strip the "{{ local_artifacts_dir }}/" prefix, leaving e.g. "bin/{{ item }}"
        rel = src.split("}}", 1)[1].lstrip("/")
        loop = task.get("loop")
        if loop is None:
            yield rel, task.get("name", "?")
        elif isinstance(loop, list):
            for item in loop:
                if isinstance(item, str):          # skip {{ }} indirection
                    yield rel.replace("{{ item }}", item).replace("{{item}}", item), task.get("name", "?")

def main(playbook, stage_dir):
    import os
    with open(playbook) as fh:
        plays = yaml.safe_load(fh)
    wanted = []
    for play in plays or []:
        for section in ("pre_tasks", "tasks", "post_tasks", "handlers"):
            for task in play.get(section) or []:
                wanted.extend(expand(task))
    missing = [(r, n) for r, n in wanted if not os.path.exists(os.path.join(stage_dir, r))]
    print(f"staging preflight: {len(wanted)} file(s) required by {playbook}")
    if missing:
        print(f"\nFAIL: {len(missing)} file(s) missing from {stage_dir}:\n")
        for rel, name in missing:
            print(f"  {rel}\n      required by task: {name}")
        print("\nAdd them to the staging loop in deploy-wasm-cluster.sh (or to the")
        print("artifact build step). Fix now - ansible would fail on this after")
        print("provisioning, with 'Could not find or access' on the Controller.")
        return 1
    print(f"ok: all {len(wanted)} present in {stage_dir}")
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: verify-staging.py <playbook.yaml> <stage_dir>")
    sys.exit(main(sys.argv[1], sys.argv[2]))
