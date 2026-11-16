Changed

- A request that carries no `max_tokens` is no longer sent to Anthropic Messages with a guessed 1024. The value comes from the new `model-catalog` setting (`default-max-tokens`, set per model by glob), then from the model's registered limit; when neither exists that candidate is skipped and another can serve.
