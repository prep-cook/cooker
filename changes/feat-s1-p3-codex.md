Internal

- Codex credentials run on the execution contract over HTTP: every client protocol is encoded by the Responses request codec and shaped with the Codex backend's rules, generation always streams from `/responses`, compact uses the native `/responses/compact`, and token counts stay local.
- A local Codex token count includes message text sent as one string.
