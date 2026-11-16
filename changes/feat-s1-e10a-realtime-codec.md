Internal

- Add the OpenAI Realtime projector and checker for live sessions: it checks client events before they are forwarded, projects event types, IDs, rate limits and per-response usage into the session stream, and counts media without decoding it. Existing routes and relay behavior are unchanged.
