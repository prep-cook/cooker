Changed

- Stream storage uses 4,096-byte frames; one exchange holds at most 256 MiB (65,536 frames), and the pool opens with 1 MiB backed. `stream.pool_bytes` must be a multiple of 4,096.
- A client waiting at the end of a media response receives the bytes already collected at once instead of after the 100 ms batch timer.
