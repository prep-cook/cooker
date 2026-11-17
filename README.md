# cooker

cooker is an AI gateway from the prep-cook teams. The project is in the development and refactoring phase.

## Run

cooker ships as the Docker image `ghcr.io/prep-cook/cooker` (tags `<version>` such as `0.3.0`, and `latest`) for linux/amd64 and linux/arm64. The container runs as uid and gid 10001; the data directory must belong to that user.

```sh
mkdir -m 700 -p data
sudo chown -R 10001:10001 data
docker run -d --name cooker -p 127.0.0.1:8317:8317 -v "$PWD/data:/data" --tmpfs /run/cooker-runtime:rw,noexec,nosuid,mode=0700,uid=10001,gid=10001 -e CK_MANAGEMENT_PASSWORD='REPLACE-WITH-A-STRONG-PASSWORD' ghcr.io/prep-cook/cooker:latest
```

An empty volume starts with the default settings on port 8317 and creates `/data/cooker.db`. Open <http://127.0.0.1:8317/management.html> and sign in with the management key you set in `CK_MANAGEMENT_PASSWORD`. Without `CK_MANAGEMENT_PASSWORD` (or `management.secret-key` in an imported `data/config.yaml`), every management request returns 403 and the dashboard cannot sign in. See [docs/INSTALL.md](docs/INSTALL.md) for the config file, upgrades, backups and building from source.

Anthropic Messages and token-counting requests accept up to 128 MB (128,000,000 bytes). Set `server.max-request-body-bytes` to enforce a smaller deployment limit; oversized requests return the client's protocol error with HTTP 413. See [body-limit configuration](docs/CONFIGURATION.md#server).
