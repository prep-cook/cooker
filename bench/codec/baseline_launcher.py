#!/usr/bin/env python3
"""Launch only the disposable baseline process in a private memory cgroup."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import signal
import tempfile


def worker(binary, args, env, index):
    group = Path(f"/sys/fs/cgroup/cooker-codec-{os.getuid()}-{os.getpid()}-{index}")
    if group.exists():
        raise RuntimeError(f"leftover baseline cgroup: {group}; inspect its cgroup.procs before removing it")
    subprocess.run(["sudo", "-n", "mkdir", str(group)], check=True)
    child = None
    read_fd, write_fd = os.pipe()
    try:
        subprocess.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}",
                        str(group / "memory.peak")], check=True)
        env = {**env, "CODEC_PRIVATE_CGROUP": str(group)}
        child = subprocess.Popen([sys.executable, __file__, "--enter", str(read_fd),
                                  str(binary), *args], pass_fds=(read_fd,), env=env)
        os.close(read_fd)
        read_fd = -1
        subprocess.run(["sudo", "-n", "sh", "-c", 'printf "%s" "$1" > "$2/cgroup.procs"',
                        "codec-cgroup-entry", str(child.pid), str(group)], check=True)
        os.write(write_fd, b"1")
        os.close(write_fd)
        write_fd = -1
        return child.wait()
    finally:
        for fd in (read_fd, write_fd):
            if fd >= 0:
                os.close(fd)
        if child is not None and child.poll() is None:
            child.kill()
            child.wait()
        subprocess.run(["sudo", "-n", "rmdir", str(group)], check=True)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--enter":
        fd = int(sys.argv[2])
        if os.read(fd, 1) != b"1":
            raise RuntimeError("parent did not authorize cgroup entry")
        os.close(fd)
        os.execv(sys.argv[3], sys.argv[3:])
    directory = Path(os.environ["CODEC_BASELINE_PROVENANCE"]).parent
    binary = directory / "old-codec-harness"
    if sys.argv[1:] == ["--describe"]:
        os.execv(binary, [str(binary), "--describe"])
    if len(sys.argv) != 5 or sys.argv[1] != "--run" or sys.argv[3] != "--round":
        raise ValueError("usage: baseline-harness --describe | --run MANIFEST --round N")
    raw = Path(sys.argv[2]).read_bytes()
    manifest = json.loads(raw)
    evidence = Path(os.environ.get("CODEC_EVIDENCE_DIR", directory / f"evidence-{os.getpid()}"))
    evidence.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "CODEC_EVIDENCE_DIR": str(evidence),
           "CODEC_MANIFEST_SHA256": hashlib.sha256(raw).hexdigest()}
    def interrupt(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGALRM, interrupt)
    signal.alarm(550)
    with tempfile.TemporaryDirectory(prefix="workload-", dir=evidence) as temporary:
        path = Path(temporary) / "workload.json"
        for index, row in enumerate(manifest["workloads"]):
            selected = {**manifest, "workloads": [row]}
            path.write_text(json.dumps(selected))
            code = worker(binary, ["--run", str(path), "--round", sys.argv[4]], env, index)
            if code:
                return code
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"baseline accounting: {error}", file=sys.stderr)
        sys.exit(2)
