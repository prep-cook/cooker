Internal

- Add mapped codecs for OpenAI, xAI and Codex image requests and responses: JSON and multipart edits, base64 and URL results, preview and completed events, and one usage snapshot per response.
- Refuse image options a destination cannot express instead of dropping them. Executors, routes and the media cutover stay outside this codec change.
