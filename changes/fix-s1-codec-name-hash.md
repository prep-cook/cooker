Changed

- Translating a request spends less CPU on JSON member lookups: every stored member name keeps a hash of its decoded text, so a lookup compares one integer per member and decodes a name only when its hash matches.
