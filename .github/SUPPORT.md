# Support

## Ask a question or report a problem

Read [Getting started](../docs/GETTING-STARTED.md), [Configuration](../docs/CONFIGURATION.md) and [Clients](../docs/CLIENTS.md) first. Search existing issues before opening a report, then use the [GitHub issue templates](https://github.com/prep-cook/cooker/issues/new/choose) for problems and feature requests.

For behaviour of the original Go server, ask in [router-for-me/CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI). State whether the problem also occurs there; [docs/DIFFERENCES-FROM-GO.md](../docs/DIFFERENCES-FROM-GO.md) lists the differences cooker keeps intentionally.

## Include diagnostic information

- The version, from `cooker --version`, or the commit if you built from source.
- The operating system and how you run cooker: the Docker image tag, or a source build.
- The tool or client that sends the request, and the relevant part of `config.yaml` with keys removed.
- Reproduction steps, expected behaviour and actual behaviour.
- The server log lines around the problem and, for dashboard problems, the browser name, version and console errors.

Remove API keys, tokens, OAuth credentials, auth files and private data before posting logs, configuration or screenshots. Report suspected vulnerabilities privately as described in [SECURITY.md](../SECURITY.md), not in a public issue.
