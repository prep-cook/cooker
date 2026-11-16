Security

- Registering an AI Studio browser relay at `/v1/ws` requires a key listed in the new `oauth.providers.aistudio.relay-keys` setting. Client keys no longer register a relay, and with no relay keys every registration is refused. Changing the list ends the connected relay sessions.
- The `oauth.providers.aistudio.ws-auth` setting is removed; a config that still sets it is refused on import. Set `relay-keys` instead.
- A relay browser that stops reading its socket no longer queues requests without limit: once 64 frames wait unwritten, new requests fail at once and move to another credential.
