Internal

- Executors start attempts through one pipeline: the request fence, a re-fence per attempt and a runtime-owned producer that reads the upstream into the response stream whatever the client does.
- The execution contract gives each attempt its response identity and exposes the capability key of a fence and of a candidate destination.
