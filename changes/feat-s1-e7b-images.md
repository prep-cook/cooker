Changed

- `POST /v1/images/generations` and `/v1/images/edits` send nothing until the upstream answer holds an image, so an upstream failure or an answer without an image moves to the next credential. The response carries `X-Ck-Stream-Id` and `Cache-Control: no-store`, has no `Content-Length` and no keepalive comments, and a streamed response resumes with `GET /v1/streams?after=v1.e.<events>`. An upstream error before the first image is returned with its own status instead of a 200 that ends in an `error` event.
- xAI image requests keep every aspect ratio xAI documents and the `1.5k` resolution. A request naming both `image` and `images`, a ratio or resolution xAI does not document, or `quality` for xAI is refused with 400 before anything is sent.
- xAI image results keep `mime_type`, `file_output` and `storage_error`.
- An image edit form whose `stream` is not `true` or `false`, whose `n` is not a number, or whose file part has no `Content-Type` is refused with 400.
