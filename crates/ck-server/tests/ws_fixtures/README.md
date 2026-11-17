# Responses WebSocket fixtures

The vectors were produced by CLIProxyAPI itself; the Rust tests never derive expected
values from Rust code.

- `ws_vectors.json`: the error frames of the handler (`buildResponsesWebsocketErrorPayload`),
  recorded from `6fecc6e`. Replayed by `src/realtime/websocket_requests_tests.rs`. The
  generator also records request normalization and a scripted end-to-end run; those cover
  the handler E9a replaced (incremental transcripts, 1012 replay closes) and are not kept.

## Regenerating

```sh
git -C <CLIProxyAPI> worktree add /tmp/cpa-ws ae3ee3be
cp go/zz_rsfix_ws_test.go /tmp/cpa-ws/sdk/api/handlers/openai/
(cd /tmp/cpa-ws && RSFIX_OUT=/tmp/rsfix-ws go test -count=1 -run TestRSFixWS ./sdk/api/handlers/openai/)
cp /tmp/rsfix-ws/ws_vectors.json .
# keep only the error_payload vectors
```
