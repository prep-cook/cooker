Fixed

- Chat streaming clients retain provider-native choice-local usage when `stream_options.include_usage` is absent or false. The extra aggregate usage chunk remains opt-in; internal accounting and buffered responses are unchanged.
