#!/usr/bin/env python3
"""Build the pinned old pair implementation without modifying its checkout."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
REVISION = "b518060ea1e43ef4b266591986ca92321e6289b5"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if len(sys.argv) != 3:
        raise ValueError("usage: build-baseline.sh BASELINE_SOURCE OUT")
    source, out = (Path(arg).resolve() for arg in sys.argv[1:])
    git = source / ".git"
    if git.is_file():
        location = git.read_text().strip().removeprefix("gitdir: ")
        git = (source / location).resolve()
    head = (git / "HEAD").read_text().strip()
    if head != REVISION:
        raise ValueError(f"baseline must have detached HEAD {REVISION}; found {head}")
    if HERE.parent.parent in out.parents or source == out or source in out.parents:
        raise ValueError("OUT must be external to both checkouts")
    out.mkdir(parents=True, exist_ok=True)
    if any(out.iterdir()):
        raise ValueError("OUT must be empty")
    inputs = [source / "Cargo.toml", source / "Cargo.lock"]
    for directory in ("crates/ck-core", "crates/ck-wire", "crates/ck-translate", "vendor/gjson"):
        inputs.extend(p for p in sorted((source / directory).rglob("*")) if p.is_file())
    checksums = {str(p.relative_to(source)): sha(p) for p in inputs}
    checksums.update({f"harness/{name}": sha(HERE / name) for name in (
        "baseline.rs", "baseline_launcher.py", "build_baseline.py")})
    source_checksum = hashlib.sha256(json.dumps(checksums, sort_keys=True).encode()).hexdigest()
    (out / "source-checksums.json").write_text(json.dumps(checksums, indent=2) + "\n")
    shutil.copy2(source / "Cargo.lock", out / "baseline-Cargo.lock")
    crate = out / "crate"
    (crate / "src").mkdir(parents=True)
    shutil.copy2(HERE / "baseline.rs", crate / "src/main.rs")
    shutil.copy2(source / "Cargo.lock", crate / "Cargo.lock")
    paths = {name: json.dumps(str(source / "crates" / name)) for name in ("ck-core", "ck-wire", "ck-translate")}
    toml = '[package]\nname = "old-codec-harness"\nversion = "0.1.0"\nedition = "2024"\n\n[workspace]\n\n[dependencies]\n'
    toml += "".join(f'{name} = {{ path = {path} }}\n' for name, path in paths.items())
    toml += 'serde_json = { version = "1", features = ["preserve_order"] }\nlibc = "0.2"\nsha2 = "0.10"\n'
    toml += f'\n[patch.crates-io]\ngjson = {{ path = {json.dumps(str(source / "vendor/gjson"))} }}\n'
    toml += '\n[profile.release]\nlto = "thin"\ncodegen-units = 1\nstrip = true\n'
    (crate / "Cargo.toml").write_text(toml)
    env = {**os.environ,
           "BUILD_CPUS": os.environ.get("BUILD_CPUS") or "8-15,24-31",
           "CARGO_BUILD_JOBS": os.environ.get("CARGO_BUILD_JOBS") or "4",
           "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR") or "/scratch/ssd/bench-target"}
    def run(args, filename):
        with (out / filename).open("w") as log:
            subprocess.run(["taskset", "-c", env["BUILD_CPUS"], *args], env=env,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
    start = datetime.datetime.now(datetime.timezone.utc).isoformat()
    # The copied lock needs only a new root entry/pruned workspace members. Offline
    # metadata updates that external lock before the locked release compilation.
    run(["cargo", "metadata", "--offline", "--format-version", "1", "--manifest-path", str(crate / "Cargo.toml")], "metadata.log")
    run(["cargo", "build", "--offline", "--locked", "--release", "--manifest-path", str(crate / "Cargo.toml")], "build.log")
    shutil.copy2(Path(env["CARGO_TARGET_DIR"]) / "release/old-codec-harness", out / "old-codec-harness")
    shutil.copy2(HERE / "baseline_launcher.py", out / "baseline_launcher.py")
    provenance = {"schema_version": 1, "revision": REVISION, "source_checksum": source_checksum,
        "compiler": subprocess.check_output([env.get("RUSTC", "rustc"), "-Vv"], text=True).strip(),
        "target": subprocess.check_output([env.get("RUSTC", "rustc"), "-vV"], text=True).split("host: ")[1].splitlines()[0],
        "profile": "release", "features": [], "build_flags": ["--offline", "--locked", "--release", "lto=thin", "codegen-units=1", "strip=true"],
        "config": {"retention": "release-at-completion", "workers": 1, "allocator": "System",
                   "instrumentation": "global-alloc+process-cpu+monotonic+smaps+private-cgroup-v2"},
        "backend_config": {"implementation": "pinned-pair-transforms", "pool": "absent"},
        "build_environment": {key: env.get(key) for key in (
            "BUILD_CPUS", "CARGO_TARGET_DIR", "CARGO_BUILD_JOBS",
            "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER",
            "CARGO_PROFILE_RELEASE_LTO", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS",
            "CARGO_PROFILE_RELEASE_OPT_LEVEL", "CARGO_PROFILE_RELEASE_STRIP",
            "CARGO_PROFILE_RELEASE_DEBUG", "CC", "CXX", "CFLAGS", "CXXFLAGS", "LDFLAGS")},
        "storage_directory": str(out), "harness_source_sha256": sha(HERE / "baseline.rs"),
        "baseline_lock_sha256": sha(out / "baseline-Cargo.lock"), "resolved_lock_sha256": sha(crate / "Cargo.lock"),
        "build_start": start, "build_end": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "build_affinity": env["BUILD_CPUS"], "build_jobs": int(env["CARGO_BUILD_JOBS"]),
        "binary_sha256": sha(out / "old-codec-harness")}
    (out / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    wrapper = out / "baseline-harness"
    import shlex
    wrapper.write_text('#!/usr/bin/env bash\nset -euo pipefail\nexport CODEC_BASELINE_PROVENANCE='
                       + shlex.quote(str(out / "provenance.json")) + '\nexec python3 -I -B '
                       + shlex.quote(str(out / "baseline_launcher.py")) + ' "$@"\n')
    wrapper.chmod(0o700)
    print(wrapper)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"baseline build: {error}", file=sys.stderr)
        sys.exit(2)
