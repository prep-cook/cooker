Internal

- The Claude executor runs a Responses compact as Anthropic's native compaction on the execution contract, replays the signed compaction block first and unchanged, and bills compaction tokens from `usage.iterations`. Server routes reach it once compact moves to the execution contract. It refuses a compaction block outside the head of a conversation message and a compaction paired with `context_management`, and never places a cache marker in the signed block.
