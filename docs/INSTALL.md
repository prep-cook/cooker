# Installing cooker

cooker is a single executable, `cooker`, with the dashboard built in. It ships as the Docker image `ghcr.io/prep-cook/cooker` (tags `<version>` such as `0.3.0`, and `latest`; linux/amd64 and linux/arm64). Configuration and OAuth credentials are persisted in SQLite. It requires a private, writable, persistent database directory. A YAML file is an optional import for the first start only; credentials are added through sign-in or the management API.

## Docker

The image runs as uid and gid 10001, exposes port 8317, uses `/data` as its working directory and starts `cooker` with no default arguments. Without `--config`, a first start imports `./config.yaml` (`/data/config.yaml`) when it exists, and the database is `cooker.db` beside it (`/data/cooker.db`).

```sh
mkdir -m 700 -p data
sudo chown -R 10001:10001 data            # the container runs as uid and gid 10001
docker run -d --name cooker -p 127.0.0.1:8317:8317 -v "$PWD/data:/data" --tmpfs /run/cooker-runtime:rw,noexec,nosuid,mode=0700,uid=10001,gid=10001 -e CK_MANAGEMENT_PASSWORD='REPLACE-WITH-A-STRONG-PASSWORD' ghcr.io/prep-cook/cooker:<version>
```

To build the image, run `docker build -t cooker .` in a checkout and use `cooker` as the image name. The build compiles BoringSSL and needs about 2 GB of memory; on a small machine add `--build-arg BUILD_JOBS=1`.
Optionally add `--build-arg CK_COMMIT="$(git rev-parse HEAD)" --build-arg CK_BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"` to the build command to embed the commit and build date.

On a fresh volume, the first start creates `/data/cooker.db` from `/data/config.yaml` when that file exists, and otherwise from the defaults with `server.port: 8317`. Later starts use `/data/cooker.db` and ignore changes to the YAML. To set client keys before the first start, add a file like this to `data/`:

```yaml
server:
  host: ""                     # listen on the container's interface
  port: 8317
access:
  api-keys:
    - "PASTE-CLIENT-KEY"       # your tools send this key to the proxy
```

Sign in at `http://127.0.0.1:8317/management.html` with the value of `CK_MANAGEMENT_PASSWORD`, or with `management.secret-key` from the imported config. Without either, the server starts, but every management request, including the dashboard sign-in, is refused with 403 ("management key not set: start the server with CK_MANAGEMENT_PASSWORD"). Keep a private copy of the keys: SQLite stores a `management.secret-key` as a bcrypt hash. [CONFIGURATION.md](CONFIGURATION.md) explains the settings.

Requests from the Docker bridge network are not local. Setting `CK_MANAGEMENT_PASSWORD` allows them; with only `management.secret-key`, the import also needs `management.allow-remote: true`. Publishing on `127.0.0.1` as above still keeps other machines out. SQLite stores sign-in and refresh state.

On a machine without a browser, such as a server you reach over SSH, keep the port published on `127.0.0.1` and open an SSH tunnel from your computer, for example `ssh -L 8317:127.0.0.1:8317 you@server`. Then open the dashboard address there. [Tailscale](MULTI-ACCOUNT.md#reach-the-proxy-from-other-machines) is an alternative. Never listen on `0.0.0.0` without client keys.

The image declares no `VOLUME`: mount `/data` as a persistent directory so published database snapshots survive replacement. Keep OS tmp writable for the private working database; it does not need persistent storage. Every acknowledged change is already in the published `/data/cooker.db`. If the container loses its tmp directory, that only means the next start seeds a fresh working database from that file; a change reported as failed may be lost with it. A YAML-only mount, remote config store or unmounted container filesystem is insufficient. On Railway, attach a Volume at `/data`; its volume ownership must permit writes by the service user (see [Railway permissions](https://docs.railway.com/volumes#permissions)). The image runs as UID/GID 10001. Older images up to 0.2.5 could create an anonymous `/data` volume; preserve or import its private legacy data before replacing it.

The service needs write access to two locations: the snapshot directory, for atomic replacement, and its OS temporary directory, for working databases. Keep both private; SQLite contains configuration secrets and OAuth tokens. Copy the `--db` file directly for a complete live backup, or use `config backup`. See [Database safety](CONFIGURATION.md#database-safety-and-backup). Publish on loopback unless other machines need access.

Browser sign-in inside a container requires the OAuth callback ports (54545 for Claude, 1455 for Codex) to be published to your machine. It is therefore usually easier to sign in from the dashboard and paste the final callback URL, or to use `--codex-device-login`, Kimi or Meta, which use device codes:

```sh
docker exec -it cooker cooker --management-base-url http://127.0.0.1:8317 --codex-device-login --no-browser
```

The running server owns `/data/cooker.db`, so the sign-in uses its management API and prompts for the management key; `--db /data/cooker.db` alone would be refused as `already owned`.

## From source

This section is for contributors. The build requires a Rust toolchain (stable, 1.88 or newer), Node 24 for the dashboard, and the tools to build BoringSSL: `cmake`, `clang` and `perl`. On Debian or Ubuntu:

```sh
sudo apt-get install -y build-essential cmake clang perl git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build the dashboard first, then the binary:

```sh
git clone https://github.com/prep-cook/cooker.git
cd cooker
(cd ui && npm ci && npm run build)
cargo build --release -p cooker
./target/release/cooker --version
```

The dashboard in `ui/dist` is not committed, so the binary build requires it to be built first. To skip Node and Rust, build the image with `docker build`. To change the dashboard, see [ui/README.md](../ui/README.md).

The Docker image includes the WebRTC media relay. A source build includes it with `cargo build --release -p cooker --features ck-server/media-relay`; without it, WebRTC calls (`POST /v1/realtime/calls`, `/v1/realtime`, `/v1/live`) answer 501. Calls also answer 501 until `oauth.providers.codex.live-media-relay.enabled` is true.

## First run from source

Set up the database by hand:

1. Create a private data directory (`mkdir -m 700 -p ~/.cooker`), generate two random keys (for example `openssl rand -hex 24`), and write a private bootstrap `config.yaml`:

   ```yaml
   server:
     host: "127.0.0.1"          # only this computer can connect
     port: 8317
   access:
     api-keys:
       - "PASTE-CLIENT-KEY"     # your tools send this key to the proxy
   management:
     secret-key: "PASTE-MANAGEMENT-KEY"  # the dashboard password
   ```

   Keep a private copy of the keys: SQLite stores the management key as a bcrypt hash. This YAML is an import shape, not the canonical persisted schema; [CONFIGURATION.md](CONFIGURATION.md) explains the settings. Keep the bootstrap file private while it exists.
2. Import the file once, then start the server on the database:

   ```sh
   cooker --db ~/.cooker/cooker.db config validate config.yaml
   cooker --db ~/.cooker/cooker.db config import config.yaml
   cooker --db ~/.cooker/cooker.db
   ```

   `--db` overrides `CK_DB`. Without either, the default is `cooker.db` beside the config file (`./config.yaml` unless `--config` names another). `--config FILE` selects the file that a first start imports; the file must exist. Without `--config`, a missing `./config.yaml` starts the server with the defaults on port 8317. Once the database exists, the server does not read or watch the file. `curl http://127.0.0.1:8317/healthz` returns `{"status":"ok"}`.
3. Connect accounts from `/management.html`, or from a terminal through the running server with `--management-base-url http://127.0.0.1:8317` (it reads the management key from `CK_MANAGEMENT_KEY` or prompts for it) and one of `--claude-login`, `--codex-login`, `--codex-device-login`, `--antigravity-login`, `--kimi-login`, `--kimi-ai-login`, `--meta-login`, `--xai-login` or `--devin-login`. Import a Vertex service account with `--vertex-import key.json`. Add `--no-browser` on a machine without a browser. With the server stopped, the same flags with `--db ~/.cooker/cooker.db` write the database directly. Sign-ins and refresh tokens are stored in SQLite, not in a watched directory.

Change configuration through the dashboard or `/admin/v1`; the old v0/v8 config endpoints are removed. `RUST_LOG=debug` enables debug logging; the default is `info`.

## Running as a system service

On a Linux server, a systemd unit such as this runs a source-built binary under a dedicated user:

```ini
# /etc/systemd/system/cooker.service
[Unit]
Description=cooker
After=network-online.target
Wants=network-online.target

[Service]
User=cooker
WorkingDirectory=/var/lib/cooker
ExecStart=/usr/local/bin/cooker --db /var/lib/cooker/cooker.db
Restart=on-failure
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/cooker /tmp

[Install]
WantedBy=multi-user.target
```

```sh
sudo useradd --system --home /var/lib/cooker --create-home cooker
sudo systemctl enable --now cooker
journalctl -u cooker -f
```

Initialize the database under `/var/lib/cooker` before starting the service, using the service user. The persistent directory must be writable for atomic snapshot publication. OS tmp must be writable for the private working database. Keep both private, and set `UMask=0077` in the unit as defense in depth. While the service runs, CLI changes go through its management API (`--management-base-url`); offline CLI writes wait until it stops. Keep the same user and OS temporary directory across restarts so that a surviving working database can resume. With `PrivateTmp=yes`, every service restart cold-starts from the published file instead; with a tmpfs `/tmp`, every reboot does the same. The published file already holds every acknowledged change. Mount this directory persistently in ephemeral deployments. No remote config store or asynchronous YAML mirror replaces it. See [backup guidance](CONFIGURATION.md#database-safety-and-backup).
