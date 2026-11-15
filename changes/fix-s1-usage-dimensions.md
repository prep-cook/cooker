Fixed

- Usage records keep the service tier Gemini reports inside `usageMetadata`.
- Usage records keep the billing dimensions providers report: Anthropic service tier, speed, inference region and web search and web fetch requests, OpenAI's 30-minute and 12-hour cache writes in their own buckets, and OpenAI Chat audio, text, image and prediction token details. A dimension the provider did not report is left out, not written as zero.
- Usage recorded from the execution contract counts Gemini tool-use prompt tokens as input.
