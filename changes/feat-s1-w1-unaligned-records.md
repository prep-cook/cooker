Changed

- Stream storage uses 16,384-byte frames; one exchange holds at most 256 MiB (16,384 frames), and the pool opens with 1 MiB backed. `stream.pool_bytes` must be a multiple of 16,384.
- A client waiting at the end of a media response receives the bytes already collected at once instead of after the 100 ms batch timer.
