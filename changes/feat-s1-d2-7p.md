Fixed

- Meta accounts serve Responses requests that carry Codex's custom `apply_patch` tool again, including `previous_response_id` continuations. The tool goes to Meta as a JSON function, and its calls come back as the custom tool under the response id cooker issued. Before this change, such a request skipped every Meta account, and a request with no other candidate failed with 400 "this account does not serve the Responses API".
- On these requests, an `apply_patch` call that cannot be returned as the custom tool ends the response with a 502 error. This covers invalid call arguments and a stream that is cut or ends inside the call.
