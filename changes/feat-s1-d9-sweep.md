Removed

- The settings `requests.nonstream-keepalive-interval` and `requests.streaming.bootstrap-retries` are removed from the configuration schema. No route used them any more: the codec routes send nothing before the first byte of the answer, and deferred xAI Chat jobs never took either.
