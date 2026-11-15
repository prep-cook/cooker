Fixed

- Usage records keep the service tier Gemini reports inside `usageMetadata`.
- Usage recorded from the execution contract counts Gemini tool-use prompt tokens as input, keeps OpenAI's 30-minute and 12-hour cache writes in their own buckets, and keeps the tier and billing dimensions providers report: Anthropic service tier, speed, inference region and web search and web fetch requests, and OpenAI audio, text, image and prediction token details. A dimension the provider did not report is left out, not written as zero.
