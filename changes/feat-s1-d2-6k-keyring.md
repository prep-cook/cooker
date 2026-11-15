Internal

- The config database holds an HMAC key ring for the envelopes that will authenticate opaque items returned to clients. The server commits a signing key before it starts listening, and fails to start when it cannot. Keys stay out of config reads, exports, history and logs, and a config restore or import never brings back a revoked key.
