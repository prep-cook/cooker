Fixed

- An Anthropic stream that reports an error mid-response, such as `overloaded_error`, now fails the attempt with that error's status (529 for overloaded) instead of a 502 for malformed upstream input; usage reported before the error is kept, and an Anthropic client receives the same error type.
- Anthropic requests with `thinking.type: between_tools` reach Anthropic unchanged instead of being refused; a route to another protocol refuses them explicitly.
