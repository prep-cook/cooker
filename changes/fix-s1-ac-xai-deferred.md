Fixed

- xAI Chat requests with `deferred: true` return a job ID and can be polled: a pending job is an empty 202, a finished job is the Chat completion, and a failed job is an error. No synchronous completion replaces them.
- Deferred Chat jobs stay bound to the creating client key and upstream credential, and their usage is charged once.
