Removed

- The settings `requests.nonstream-keepalive-interval` and `requests.streaming.bootstrap-retries` are removed from the configuration schema. No route used them any more: the codec routes send nothing before the first byte of the answer, and deferred xAI Chat jobs never took either. YAML import and management writes now reject them as unknown keys, so a stored config that still sets one must drop it before upgrading.
