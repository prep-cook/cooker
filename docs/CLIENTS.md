# Use cooker with your tools

The examples use `http://127.0.0.1:8317` and `your-client-key`. Replace them with your installation's address and client key. The dashboard at `/management.html` provides a Use with tools dialog, on the Client keys page, that shows both values with copy buttons. SQLite is the proxy's durable config authority; do not take current settings from an old import YAML. Choose a model ID from the dashboard's Models page or from `GET /v1/models`.

## Claude Code

[Claude Code's gateway guide](https://code.claude.com/docs/en/llm-gateway-connect) uses the Anthropic Messages format. Set the base URL without `/v1`; Claude Code appends `/v1/messages`. Claude Code sends the token as bearer authorization. Optionally, set `ANTHROPIC_MODEL` to an ID from the proxy.

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8317
export ANTHROPIC_AUTH_TOKEN=your-client-key
export ANTHROPIC_MODEL=<model-id>
claude
```

For a persistent personal setup, add this to `~/.claude/settings.json`:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8317",
    "ANTHROPIC_AUTH_TOKEN": "your-client-key",
    "ANTHROPIC_MODEL": "<model-id>"
  }
}
```

### Claude Code versions

With a Claude account, cooker forwards Claude Code's own identity, its `User-Agent` and its SDK and Node versions, when Claude Code is 2.1.280 or newer within major version 2. A release newer than 2.1.280 must send both its `X-Stainless-Package-Version` and `X-Stainless-Runtime-Version` headers, as Claude Code does. cooker forwards that release's session-title and quota-probe requests the same way.

With `stabilize-device-profile: true`, 2.1.x releases from 2.1.280 still count as Claude Code, so cooker does not cloak them, but they go upstream with the stabilized 2.1.280 identity. 2.2 and later are cloaked, and the Stainless requirement does not apply to them. cooker sends other clients and older Claude Code releases upstream as Claude Code 2.1.280.

When a `claude-cli` client is not forwarded, the log reports this at most once per account and version. The entry names the account by its auth index, the one the dashboard and the Management API show, and states when missing Stainless headers are the reason:

```text
claude: Claude Code 2.1.220 on credential 1f3a9c0b7d2e4a68 is not passed through; requests use the claude-cli/2.1.280 (external, cli) identity. ...
```

Anthropic refuses Claude Code releases that are too old for a model. The error reaches your client as a 400:

```text
Claude Code 2.1.236 does not support this model; version 2.1.251 or newer is required. Run 'claude update', or update the Claude desktop app, then try again.
```

Its `error.details.error_code` is `claude_code_version_too_old`. From Claude Code itself, run `claude update`. From another client, or from a Claude Code release that is not forwarded, the identity cooker sends is older than the model requires. Until a cooker release raises the default identity (Claude Code 2.1.280), set a newer one in the `upstream.claude.header-defaults` settings through the dashboard or `/admin/v1`. The YAML below uses the YAML shape described in [Configuration](CONFIGURATION.md); it is for first import, not a watched file. Use the version that `claude --version` prints and the `X-Stainless-Package-Version` and `X-Stainless-Runtime-Version` headers that release sends:

```yaml
upstream:
  claude:
    header-defaults:
      user-agent: "claude-cli/<version> (external, cli)"
      package-version: "<X-Stainless-Package-Version>"
      runtime-version: "<X-Stainless-Runtime-Version>"
```

The version in `user-agent` also becomes the lowest Claude Code version that cooker forwards as itself. Editing an imported file afterward has no effect.

### GPT in Claude Code

Configure an OpenAI API key, then choose an available GPT model from `/v1/models`. A connected Codex account also works; check OpenAI's terms first. Claude Code sends Messages requests, and the proxy translates them to the selected provider's format.

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8317
export ANTHROPIC_AUTH_TOKEN=your-client-key
export ANTHROPIC_MODEL='<gpt-model-id>'
export ANTHROPIC_DEFAULT_HAIKU_MODEL='<gpt-model-id>'
# Example for a model with at least a 200,000-token context window:
export CLAUDE_CODE_MAX_CONTEXT_TOKENS=200000
export CLAUDE_CODE_AUTO_COMPACT_WINDOW=180000
claude
```

`ANTHROPIC_DEFAULT_HAIKU_MODEL` also routes Claude Code's small-model calls; otherwise those calls may still ask for Haiku. A different available GPT model can fill that slot. If you use Claude Code's named model tiers, also set `ANTHROPIC_DEFAULT_OPUS_MODEL`, `ANTHROPIC_DEFAULT_SONNET_MODEL` and `ANTHROPIC_DEFAULT_FABLE_MODEL` to available proxy models.

Claude Code's [environment reference](https://code.claude.com/docs/en/env-vars) defines both variables. `CLAUDE_CODE_MAX_CONTEXT_TOKENS` sets the window Claude Code assumes; the upstream's context limit still applies. `CLAUDE_CODE_AUTO_COMPACT_WINDOW` accepts an integer token count from 100,000 to 1,000,000. Check the selected model's provider limit and set a compaction window below it. Set both variables in Claude Code's environment.

## Codex CLI

[Codex custom model providers](https://developers.openai.com/codex/config-advanced) use the OpenAI Responses format. Add this to `~/.codex/config.toml`, replace `<model-id>`, and set the environment variable. WebSocket support is documented but optional, so this setup uses the normal HTTP endpoint.

```toml
model = "<model-id>"
model_provider = "cooker"

[model_providers.cooker]
name = "cooker"
base_url = "http://127.0.0.1:8317/v1"
env_key = "CK_API_KEY"
wire_api = "responses"
```

```bash
export CK_API_KEY=your-client-key
codex
```

## Gemini CLI

[Gemini CLI's official repository](https://github.com/google-gemini/gemini-cli/issues/1679#issuecomment-3293504913) confirms `GOOGLE_GEMINI_BASE_URL` for a custom Gemini endpoint. `GEMINI_API_KEY` is the documented key variable. The CLI uses Gemini `/v1beta` routes.

```bash
export GOOGLE_GEMINI_BASE_URL=http://127.0.0.1:8317
export GEMINI_API_KEY=your-client-key
gemini
```

Choose `<model-id>` with Gemini CLI's `/model` command. Not verified against the official documentation: a model environment variable for a custom endpoint.

## Amp

[Amp Model Routing](https://ampcode.com/docs/customize/model-routing#custom-url-connections) calls Custom URL connections from Amp's servers. Expose the proxy at a public HTTPS address first. In Model Routing, choose Add, then Custom URL. Use one of the following configurations and map an Amp model to `<model-id>`.

```text
Anthropic Messages
API format: anthropic-messages
Base URL: https://proxy.example.com
API key: your-client-key

OpenAI Responses
API format: responses
Base URL: https://proxy.example.com/v1
API key: your-client-key
```

Amp sends the API key as bearer authorization. Use Check Access, or run:

```bash
amp config model-providers check-access --provider-model <provider/model>
```

## OpenCode

[OpenCode's provider guide](https://opencode.ai/docs/providers#custom-provider) documents custom OpenAI-compatible providers. Add this to `opencode.json`, replace `<model-id>`, then select `cooker/<model-id>` with `/models`. This setup uses OpenAI Chat Completions.

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "cooker": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "cooker",
      "options": {
        "baseURL": "http://127.0.0.1:8317/v1",
        "apiKey": "your-client-key"
      },
      "models": {
        "<model-id>": { "name": "<model-id>" }
      }
    }
  }
}
```

## Factory Droid

[Factory's custom model guide](https://docs.factory.ai/model-independence/byok) uses `~/.factory/settings.json`. This example uses OpenAI Chat Completions. Replace `<model-id>`, then choose it with `/model`.

```json
{
  "customModels": [
    {
      "model": "<model-id>",
      "displayName": "cooker",
      "baseUrl": "http://127.0.0.1:8317/v1",
      "apiKey": "your-client-key",
      "provider": "generic-chat-completion-api"
    }
  ]
}
```

For Anthropic Messages, use `provider: "anthropic"` and `baseUrl: "http://127.0.0.1:8317"` instead.

## Cline, Roo Code and Kilo Code

[Cline](https://docs.cline.bot/provider-config/openai-compatible), [Roo Code](https://docs.roocode.com/providers/openai-compatible), and [Kilo Code](https://kilo.ai/docs/ai-providers/openai-compatible) accept the same OpenAI Chat Completions fields. Open the extension's provider settings and enter:

```text
Provider or provider API: OpenAI Compatible
Base URL: http://127.0.0.1:8317/v1
API key: your-client-key
Model or model ID: <model-id>
```

Kilo also offers OpenAI Responses and Anthropic Messages custom provider formats. Cline documents an Anthropic Use custom base URL option. The cited Roo Code page documents only its OpenAI-compatible setup.

## Cursor

[Cursor's current key guide](https://cursor.com/help/models-and-usage/api-keys) confirms Cursor Settings, Models, and the OpenAI key field. It also states that every request goes through Cursor's servers, so `127.0.0.1` cannot work. Expose the proxy over HTTPS first.

```text
Cursor Settings > Models
OpenAI API key: your-client-key
Override OpenAI Base URL: https://proxy.example.com/v1
Custom model: <model-id>
```

Not verified against the official documentation: the current official guide does not document Override OpenAI Base URL or Custom model. Confirm that these fields exist in your installed Cursor version before using this setup.

## Zed

[Zed's API access guide](https://zed.dev/docs/ai/use-api-access#openai-compatible) supports custom OpenAI-compatible providers. Add this to Zed's `settings.json`, replace `<model-id>`, and set the key variable as shown below. Zed uses Chat Completions by default.

```json
{
  "language_models": {
    "openai_compatible": {
      "cooker": {
        "api_url": "http://127.0.0.1:8317/v1",
        "available_models": [
          {
            "name": "<model-id>",
            "display_name": "cooker",
            "max_tokens": 128000
          }
        ]
      }
    }
  }
}
```

```bash
export CK_API_KEY=your-client-key
```

Set `capabilities.chat_completions` to `false` in the model entry to use OpenAI Responses. Zed also documents `language_models.anthropic_compatible` with `api_url: "http://127.0.0.1:8317"` for Anthropic Messages.

## Continue

[Continue's OpenAI provider guide](https://docs.continue.dev/customize/model-providers/top-level/openai#openai-api-compatible-providers) uses `apiBase`, `apiKey`, and `model` in `config.yaml`. This setup uses OpenAI Chat Completions.

```yaml
name: cooker
version: 1.0.0
schema: v1
models:
  - name: cooker
    provider: openai
    model: <model-id>
    apiBase: http://127.0.0.1:8317/v1
    apiKey: your-client-key
```

## Aider

[Aider's OpenAI-compatible guide](https://aider.chat/docs/llms/openai-compat.html) documents these variables and the `openai/` model prefix. It uses OpenAI-compatible requests.

```bash
export OPENAI_API_BASE=http://127.0.0.1:8317/v1
export OPENAI_API_KEY=your-client-key
aider --model openai/<model-id>
```

Not verified against the official documentation: an Anthropic custom-base variable. Aider documents `ANTHROPIC_API_KEY`, but its current options reference does not document `ANTHROPIC_API_BASE`.

## SDKs

All examples replace `<model-id>` with an ID from the proxy.

### OpenAI Python

The example below uses the [official Python SDK](https://github.com/openai/openai-python) with OpenAI Responses.

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8317/v1", api_key="your-client-key")
response = client.responses.create(model="<model-id>", input="Hello")
print(response.output_text)
```

### OpenAI JavaScript

The example below uses the [official JavaScript SDK](https://github.com/openai/openai-node/blob/main/docs/authentication.md) with camel-case `baseURL` and `apiKey`.

```javascript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://127.0.0.1:8317/v1",
  apiKey: "your-client-key",
});
const response = await client.responses.create({ model: "<model-id>", input: "Hello" });
console.log(response.output_text);
```

### Anthropic Python

The example below uses the [official Python SDK](https://github.com/anthropics/anthropic-sdk-python) with Anthropic Messages and a base URL without `/v1`.

```python
import anthropic

client = anthropic.Anthropic(base_url="http://127.0.0.1:8317", api_key="your-client-key")
message = client.messages.create(
    model="<model-id>", max_tokens=256,
    messages=[{"role": "user", "content": "Hello"}],
)
print(message.content[0].text)
```

### Anthropic JavaScript

The example below uses the [official JavaScript SDK](https://github.com/anthropics/anthropic-sdk-typescript) with `baseURL` and `apiKey`.

```javascript
import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic({
  baseURL: "http://127.0.0.1:8317",
  apiKey: "your-client-key",
});
const message = await client.messages.create({
  model: "<model-id>", max_tokens: 256,
  messages: [{ role: "user", content: "Hello" }],
});
console.log(message.content[0].text);
```

### Google Gen AI Python

The [official Python SDK source](https://github.com/googleapis/python-genai/blob/main/google/genai/client.py) accepts `api_key` and `http_options`. The example uses Gemini `/v1beta` routes.

```python
from google import genai
from google.genai import types

client = genai.Client(
    api_key="your-client-key",
    http_options=types.HttpOptions(base_url="http://127.0.0.1:8317"),
)
response = client.models.generate_content(model="<model-id>", contents="Hello")
print(response.text)
```

The published Python guide describes custom base URLs only for its enterprise mode. The SDK's current source accepts the combination above for the Gemini Developer API too.

### Google Gen AI JavaScript

The [official JavaScript SDK source](https://github.com/googleapis/js-genai/blob/main/src/client.ts) accepts `apiKey` and `httpOptions.baseUrl`.

```javascript
import { GoogleGenAI } from "@google/genai";

const client = new GoogleGenAI({
  apiKey: "your-client-key",
  httpOptions: { baseUrl: "http://127.0.0.1:8317" },
});
const response = await client.models.generateContent({
  model: "<model-id>", contents: "Hello",
});
console.log(response.text);
```

## Deferred xAI Chat

Send `POST /v1/chat/completions` with an xAI model, `deferred: true`, and no streaming. The response is xAI's `request_id`, not a completion. Poll `GET /v1/chat/deferred-completion/{request_id}` with the same client key:

- `202` with an empty body: pending; poll again later.
- `200` with a Chat completion: completed.
- An error status: the poll failed; no synchronous generation replaces it.

The job stays bound to its creating client key and upstream credential for 3 hours after the last poll. Unknown, expired and other callers' IDs return `404` without an upstream request. Bindings live in process memory, so restarting cooker loses them. OAuth Chat proxy credentials and `stream: true` are rejected.

xAI keeps a completed result for 24 hours and releases it once. Cooker never retries a sent create or poll, because a lost response may already have created or consumed the job. See xAI's [deferred Chat guide](https://docs.x.ai/developers/advanced-api-usage/deferred-chat-completions) and [REST reference](https://docs.x.ai/developers/rest-api-reference/inference/chat-completions).

## Check it works

```bash
curl -sS http://127.0.0.1:8317/v1/models \
  -H 'Authorization: Bearer your-client-key'
```

A `401` response means the client key is wrong. If a cloud-hosted tool cannot connect, use a public HTTPS address such as `https://proxy.example.com` instead of `127.0.0.1`.
