#!/usr/bin/env python3
"""Generate YAML field shapes from the pinned Go reference, not example defaults.

Run: python3 generate_schema.py /path/to/CLIProxyAPI
The checked-in artifact makes normal Rust builds independent of Go and Python.
"""
import copy
import json
from pathlib import Path
import re
import subprocess
import sys

reference = Path(sys.argv[1])
revision = subprocess.check_output(["git", "-C", str(reference), "rev-parse", "HEAD"], text=True).strip()
assert revision.startswith("ae3ee3be"), "Use the pinned parity reference"
structs = {}
aliases = {}
# Go's plugin section names pluginstore types; they are resolved, then the section is dropped below.
for directory, prefix in [("internal/config", ""), ("internal/registry", "registry."), ("internal/pluginstore", "sdkpluginstore.")]:
    for file in sorted((reference / directory).glob("*.go")):
        if file.name.endswith("_test.go"):
            continue
        text = file.read_text()
        for name, body in re.findall(r"type (\w+) struct \{(.*?)^\}", text, re.S | re.M):
            fields = {}
            for line in body.splitlines():
                match = re.match(r'\s*\w+\s+([^`]+?)\s+`[^`]*yaml:"([^",]+)', line)
                if match and match[2] != "-":
                    fields[match[2]] = match[1].strip()
            structs[prefix + name] = fields
        for name, target in re.findall(r"^type (\w+) = ([\w.]+)$", text, re.M):
            aliases[prefix + name] = prefix + target

def shape(type_name):
    if type_name in aliases:
        return shape(aliases[type_name])
    if type_name.startswith("*"):
        return {"optional": shape(type_name[1:])}
    if type_name.startswith("[]"):
        return {"list": shape(type_name[2:])}
    if type_name.startswith("map[string]"):
        return {"map": shape(type_name[11:])}
    if type_name in structs:
        return {"fields": {key: shape(value) for key, value in structs[type_name].items()}}
    primitives = {"bool": "bool", "int": "int", "int64": "int", "uint16": "port", "string": "string",
                  "time.Duration": "duration", "any": "opaque", "yaml.Node": "opaque",
                  "DisableImageGenerationMode": "image-mode"}
    assert type_name in primitives, f"Unresolved Go type: {type_name}"
    return primitives[type_name]

schema = shape("Config")
schema["fields"].update(shape("SDKConfig")["fields"])

def take(path):
    node = schema
    parts = path.split(".")
    for part in parts[:-1]:
        node = node["fields"][part]
    return node["fields"].pop(parts[-1])

def merge(old, new):
    """Go's v8 table maps several roots onto one struct (`claude` and `claude-code` to
    `upstream.claude`): their fields join."""
    if isinstance(old, dict) and isinstance(new, dict) and "fields" in old and "fields" in new:
        for key, value in new["fields"].items():
            old["fields"][key] = merge(old["fields"][key], value) if key in old["fields"] else value
        return old
    assert old == new, f"conflicting shapes {old} and {new}"
    return new

def put(path, value):
    node = schema
    parts = path.split(".")
    for part in parts[:-1]:
        node = node["fields"].setdefault(part, {"fields": {}})
    fields = node["fields"]
    fields[parts[-1]] = merge(fields[parts[-1]], value) if parts[-1] in fields else value

source = (reference / "internal/config/config_v8.go").read_text()
prefixes = source.split("prefixes := []configPath{", 1)[1].split("var out", 1)[0]
for old, current in re.findall(r'\{"([^"]+)", "([^"]+)"\}', prefixes):
    put(current, take(old))
# Home mode's credential lease settings were removed with Home mode.
schema["fields"].pop("credentials")
# Plugins were removed; extending the gateway means changing its source.
schema["fields"].pop("plugins")
# cooker keeps credentials in the database; nothing reads a credential directory setting.
schema["fields"]["oauth"]["fields"].pop("auth-dir")
# Client aliases are removed, not part of the canonical writable schema.
schema["fields"]["oauth"]["fields"]["providers"]["fields"]["codex"]["fields"].pop("optimize-multi-agent-v2", None)
families = source.split("var v8KeyFamilies = []configPath{", 1)[1].split("func buildV8Paths", 1)[0]
shared = ["priority", "prefix", "proxy-url", "headers", "models", "excluded-models", "disable-cooling", "request-retry", "request-scoped-errors"]
for old, family in re.findall(r'\{"([^"]+)", "([^"]+)"\}', families):
    key = take(old)["list"]
    if family == "openai-compatibility":
        group = key
        group["fields"]["keys"] = group["fields"].pop("api-key-entries")
    else:
        fields = key["fields"]
        group = {"fields": {"name": "string", "base-url": fields.pop("base-url", "string")}}
        for field in shared:
            if field in fields:
                group["fields"][field] = copy.deepcopy(fields[field])
        group["fields"]["keys"] = {"list": key}
    put("api-keys." + family, {"list": group})
schema["fields"]["config-version"] = "version"
# cooker additions (docs/DIFFERENCES-FROM-GO.md). Go starts with them and ignores
# them; a config edit through its v8 Management API moves them into comments.
put("oauth.providers.codex.chatgpt-keep-alive", "bool")
put("observability.usage.claude-cache-telemetry", "bool")
put("routing.cooldown.max-trusted-cooldown", "string")
providers = {"fields": {"exclude": {"list": "string"}, "order": {"list": "opaque"}}}
put("routing.providers", copy.deepcopy(providers))
schema["fields"]["worker-threads"] = "int"
schema["fields"]["artificial-analysis"] = {"fields": {"api-key": "string", "url": "string", "cache-file": "string"}}
schema["fields"]["model-catalog"] = {"list": {"fields": {"match": "string", "overrides": {"fields": {"default-max-tokens": {"optional": "int"}}}}}}
schema["fields"]["model-routers"] = {"list": {"fields": {"name": "string", "tier": "string", "judge": "opaque", "models": {"list": "opaque"}, "fallback-models": "int", "providers": providers}}}
realtime = ["ping-interval-seconds", "read-timeout-seconds", "write-timeout-seconds", "max-sockets-per-key"]
schema["fields"]["realtime"] = {"fields": {key: "int" for key in realtime}}
schema["fields"].pop("home", None)
Path(__file__).with_name("schema.json").write_text(json.dumps(schema, indent=2) + "\n")
print(f"Generated schema from {revision}")
