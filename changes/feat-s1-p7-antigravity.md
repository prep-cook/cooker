Changed

- On Antigravity Claude models, clients receive thinking signatures exactly as the upstream sent them, and the signatures they send back go upstream unchanged. A thinking block without a valid Claude signature is dropped; no cached signature is filled in.
