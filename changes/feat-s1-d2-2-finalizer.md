Internal

- The server's attempt loop can run a decoded request on the execution contract: each candidate is re-fenced, skipped with a logged reason when its destination refuses the request, or sent; a failed attempt settles its usage record once and discards its response stream before the next attempt starts. No route uses it yet.
