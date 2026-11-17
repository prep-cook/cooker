Internal

- The Responses response codec shows its client only the exchange's own response id, never an upstream's, and takes the producing account for later provenance checks.
- The client request fence closes against a `client` capability that no request encoder accepts, so every attempt re-validates the request for its own destination before anything is sent.
