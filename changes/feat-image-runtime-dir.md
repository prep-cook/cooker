Changed

- The Docker image sets `XDG_RUNTIME_DIR=/run/cooker-runtime`, so the stream pool opens without extra configuration. Mount a tmpfs there to keep pooled bodies in memory: `--tmpfs /run/cooker-runtime:rw,noexec,nosuid,mode=0700,uid=10001,gid=10001`. Without the mount the pool uses the container's disk.
