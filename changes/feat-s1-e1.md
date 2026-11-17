Internal
- Add incremental multipart decoding for the media cutover, preserving ordered parts, filenames and binary bytes in the stream pool and rejecting invalid or oversized uploads before request admission.
- Accept quoted-pair boundaries and validate closing-delimiter padding and line endings before admitting multipart uploads.
