# State and concurrency

**Accepted design contract; implementation is in progress.** This document records the configuration and state model agreed for cooker. It does not claim that every path in the current tree already conforms. [Configuration](CONFIGURATION.md) describes the operational interfaces.

Function names, type names, method signatures and file layout can change. The ownership, persistence, data-flow, notification and lifecycle model below is not an implementation preference: every refactor must preserve it. Do not weaken it to accommodate a new caller or make a check pass. Changing the model requires an explicit architecture decision and the corresponding contract and test updates before implementation.

## One portable state file

**The configured database is one file that a user can copy at any time, including while the service runs, and use for a complete supported-state recovery. This existing guarantee must not regress.**

- Copying the published file does not require a backup API, an export command or a service stop.
- The copy needs no WAL, SHM, rollback journal, live workspace, lock or owner metadata, or materialized credential cache beside it.
- Publication replaces a complete immutable image. A concurrent copy receives a complete older or newer version, never mixed pages.
- The service acknowledges success only after durable publication. A copy already in progress can still finish with the older complete version.
- Private transient files used for construction or atomic replacement are allowed. They are not backup dependencies, and they do not require users to collect a directory of files.
- The image contains the supported persistent configuration and credential state. It is not an installer bundle, and it contains no application binaries or unrelated external assets.

```text
persistent configuration directory
  cooker.db                     one required portable state artifact

private OS temporary storage
  working checkpoint          owned recovery data, not a backup companion
  ownership bookkeeping       never needed to restore cooker.db
```

Keep secret-bearing files private before publication. Containers mount the persistent directory, not an old database inode, so atomic replacement remains possible.

## One logical owner

A resident application service owns canonical state, command admission, persistence, current runtime views, callback subscriptions and workspace lifetime. The current server is one process with multiple threads, and this service belongs inside that process. Providing a common interface does not require a new daemon or a cross-process shared-memory model.

External online clients, including CLI import and login persistence, submit authenticated management requests. Internal OAuth, refresh and scheduler work submits commands directly to the same service and does not need HTTP loopback. When the service is stopped, an offline tool can use the same commit implementation only after it obtains exclusive ownership.

An external login checks ordinary owner admission before it starts upstream work. It captures an authorized immutable source view for the relevant merge and precondition checks. Provider options come from the authorized resource export through the existing SQL-free importer and compiler, never from a competing local store. A caller that needs existing file contents before granting, such as SIWC's path validation and device-ID selection, must hydrate its captured target in private scratch before those effects. An explicit target is resolved against the owner's managed-directory descriptor without opening that path on the client. Unsupported aliases and outside or missing targets fail before granting. Provider writers render only in private client scratch and submit their logical batch conditionally. A deleted or replaced captured source must not be resurrected or overwritten. A declared or captured destination that is missing from the private scratch after a successful renderer can represent an explicit removal; it never represents an arbitrary read failure. The owner validates every save and delete target and every raw-source condition before one mixed commit. It preserves removals in pending and full-state images, and it reconciles runtime revocation before acknowledgement. Callers that enumerate legacy aliases before declaring paths, such as Claude, must receive the relevant captured filename namespace before that enumeration; final-write hydration alone is too late. A synchronous provider save callback must not return success merely because scratch was written. It must wait for the authenticated owner's acknowledgement on an owned blocking writer, not on the async executor. One private operation permit owns hydration, render, read and acknowledgement through completion; cursor CAS alone does not protect a shared scratch directory. This permit carries no canonical state, rejects overlap and reentry without blocking on itself, and is distinct from the short-lived cursor lock. The cursor advances only to acknowledged own output; it never advances by rereading and adopting an independent remote replacement. Scratch remains owned until that writer settles. This ordinary external write gains no internal grant-preservation authority and no client-controlled bypass.

Do not expose a writable SQL or backend handle to these adapters. Private table-specific helpers are fine; independently writable modules are not. A failed online request or authentication attempt never authorizes a fallback local write. SQLite's transaction lock does not replace application-level ownership.

## Current state lives in memory

Queries use immutable memory views. A reader can keep a snapshot for a whole request while a later version is published; it does not open SQLite or wait for disk I/O under the state lock.

The canonical persistent snapshot is more complete than a runtime configuration struct. Preserve resources and their IDs and order, raw credential documents and metadata, configuration history and persisted cooldowns. Derive runtime configuration and scheduling views from that state; do not create a second independently mutable authority.

Keep domains distinct. Restoring configuration history cannot revive a revoked credential. A cooldown update is not a configuration change. Persisting a granted token and admitting that token for runtime use are different transitions. Consumers receive only their authorized views, not a broadcast of all secrets.

Volatile request counters, active leases and immediate scheduler feedback remain component-owned execution state. Their durable cooldown checkpoints still go through the sole writer. That requirement neither permits another mutable configuration authority nor requires every request to wait on disk persistence.

## One commit sequence

```text
command + accepted state
          |
          v
check preconditions and validate candidate
          |
          v
encode complete standalone SQLite image
          |
          v
persist recovery checkpoint
          |
          v
publish configured file, sync file and directory metadata
          |
          v
close transient storage resources
          |
          v
replace current memory snapshot
          |
          v
dispatch relevant ordered callbacks
          |
          v
return success
```

Serialize mutations without holding the memory read/write lock over encoding, filesystem work or callbacks. Credential operations admit the same service command before taking runtime reload or per-credential guards; their internal persistence uses that admitted context instead of acquiring the owner again. Stable document locks may cover token preparation and grants, but required configuration projections do not acquire those locks. Long network grants run outside the commit gate; when admitted, their results must still satisfy the relevant registration and resource preconditions.

A credential batch holds no document lock while it validates and commits. A refresh of an existing credential document takes that document's lock, then restores the document through the store command before any upstream work, and keeps the lock until its result commits. Under `disk` and one store command, the batch therefore tries each affected document lock without waiting and explicitly unlocks it at once: a free lock means no refresh of that document is in flight, and none can restore the document before the batch commits. A held lock means a refresh is in flight. The batch then drops the command and `disk`, waits for that document's lock alone (shutdown stops the wait), explicitly unlocks it, and repeats the whole attempt, including every source condition. After 16 busy attempts, it returns `store_failed` with the message "the credential lock kept changing hands". The attempt that commits validates, checks conditions and publishes under one command, so the batch stays all or nothing, and its descriptor use does not grow with the number of documents. This covers existing-document token refresh only: an SIWC authorization-code exchange runs before any document lock and can overlap a batch, but it commits through the same `disk` and store command. A builtin login save repairs every document's cache file after its result commits, and it probes the same way: under `disk` and the store writer it tries and unlocks each document lock before it writes, and on a held lock it drops both, waits for that lock alone and repeats the repair. Because the login result has already committed at admission, the save has no attempt limit: failing would report a failed sign-in for a stored credential. Shutdown stops the wait instead; the stored credential stays, and the next start rebuilds every cache file from the database when it initializes the credential store.

Invalid input, stale preconditions and dry runs do not advance accepted state. Failure to publish the configured file is not a successful save. Once a newer checkpoint exists, retain it as explicit **publication-pending recovery state**: refuse ordinary new mutations until that checkpoint is durably published and reconciled. Never build a later full-state image from older accepted memory, because that can erase a rotated token whose previous value is no longer valid.

An already in-flight, non-repeatable canonical file-backed grant result must be preserved when the same owner can retain it against the latest pending checkpoint, with the relevant admission and version checks retained. Pause new grant starts. Do not discard retainable completed grants merely because publication is pending, and do not reuse an affected invalidated credential from an older view. Resolving pending publication precedes normal write admission and successful-change notifications. Pending state does not require a resident SQLite connection.

Builtin login results enter owner-held execution state before any cache hydration or legacy merge or write. Their legacy writers render in a private temporary directory hydrated from the latest canonical or pending documents. A failed local effect retains the original returned provider value for recovery, not a new exchange. Existing sources for that provider are conservatively withheld from dispatch until a legacy renderer identifies its exact destination. For retryable failures, this provider-wide withholding lasts until a later owner admission drains the held queue; it has no bounded timeout and no background retry. The consumed result keeps its issued session authorization independently of the UI session TTL or cancellation. Recovery checks captured destination sources. Retryable local I/O and storage errors retain the held result. Terminal validation rejection, changed or deleted captured destinations, or lost authorization visibly fail the login and dispose of it without applying over the current source or blocking unrelated writes. Disposal releases that holder's provider-wide withholding. It keeps an exact-source dispatch fence on a resolved, unchanged, synthesizable captured destination until replacement, deletion or a later successful login settles it. A changed destination or an already-invalid or unsynthesized source needs no extra fence; unrelated commits cannot release the exact-source fence. SIWC derives binding identity and bytes from canonical or pending documents, and retains those bytes before cache identity, hydration or document-lock effects. Temporary render files are private, are cleaned with their operation, and are never part of the portable backup.

**Accepted completion-failure decision:** “If the disk fails at the exact moment a login or token refresh completes, it is acceptable for that login to fail and require re-login, provided: never report success, never overwrite already-durable data from stale memory, never deadlock, never resurrect deleted/revoked credentials.” Completion-time local storage/rendering failure may therefore require re-login, but must not permanently block unrelated writes. This does not weaken durability for acknowledged state.

**Known-limitation ledger:** unrendered builtin results remain in owner-lifetime memory until complete bytes enter the retained checkpoint; owner exit during local completion failure can lose them. Retryable pre-destination failures conservatively withhold existing file-backed sources of the provider until a later owner admission drains the queue. Without a later admission, withholding can last indefinitely; it is not a background timed retry. Legacy writer string errors use explicit known validation-code classification. Opaque effect errors stay retryable, and additional validation codes require classification or a typed writer migration. Read-only commands under a live owner see its latest completed publication, which can trail an in-flight write. Online and dashboard Claude logins render against top-level credential files only, so a nested legacy alias remains a separate credential. Atomic publication, private permissions and the native ownership lock are exercised by the full suite on Linux. macOS and Windows CI run only the watcher, idle and installer subsets. On macOS and Windows, the installer job shows the lock working across processes: an offline import into the database of the running installed server failed on both platforms, and the same import succeeded once the server had stopped. Their durability and ACL semantics rest on the platform primitives rather than on tests.

The configured SQLite snapshot remains the complete single-file backup of all **supported persistent state**.

A checkpoint written before failed publication can be recovered on resume even though the caller saw an error; the service must not claim to have rolled it back. If publication is still unresolved at shutdown, preserve the recovery workspace and report incomplete cleanup instead of deleting the only newer state. Temporary-storage loss can still lose unacknowledged work; the single published file remains the recovery guarantee for acknowledged state.

An ordinary subscriber failure does not undo a committed state. Report the failure and continue notifying eligible ordinary subscribers. Required canonical or runtime installation is different: it is part of success, it precedes ordinary notifications, and it must propagate failure. A post-durability installation failure does not roll back the snapshot. Retain the candidate and its original versioned configuration event, withhold ordinary success notifications, and refuse ordinary mutations until the required projection completes. A retry must not infer a no-op from an already advanced canonical pointer and lose that event. Initial required-registration failure removes the registration and does not leave a previously unbound runtime bound. Commit success does not imply that an ordinary subscriber's expensive downstream work finished.

**Commit cost.** Every accepted commit, including a credential refresh or a coalesced cooldown checkpoint, encodes a complete image of all credentials and the whole configuration history; configuration history has no retention limit. A commit therefore costs more than a row update and queues behind the single commit authority. Measured with 2,000 credentials (release builds, loopback): a setting commit took about 1.7× and a credential commit about 1.4× the WAL design's time, 50 concurrent credential uploads finished in about 4.5 s instead of about 2.1 s, and setting commits slowed by about 12% over 1,000 revisions.

## Functional core, explicit effects

Snapshot export has two logical transformations: obtain immutable complete state, then encode that value into a fresh SQLite image. These are roles, not required function names.

The encoder does not read or copy an old database. Use mature SQLite construction and serialization support, not a custom SQLite file-format writer. SQLite connections exist only for explicit bootstrap, migration or encoding operations and are closed afterward; no idle disk connection or page cache is maintained. Filesystem synchronization and atomic replacement live at the persistence edge.

Isolated computation receives all business inputs and returns a result or an error. It must not look up a global runtime or configuration object, reread live SQL, infer a mutable environment or current directory, or capture the owner's mutable state. Explicit adapters supply required file, network and environment input. Pass the minimum required data, not all credentials.

Results carry the relevant input preconditions. The owner accepts or rejects them; finishing later does not entitle a worker to overwrite newer state. Unrelated changes must not invalidate otherwise valid results merely because a global number changed.

## Owned function callbacks

The service registers, invokes and releases subscriptions. Registration establishes an initial view and future notifications atomically with respect to publication.

Callbacks receive an immutable authorized view, its version and change category, and use that input instead of rereading ambient configuration. Notification dispatch follows commit order and runs without the memory or subscriber-list lock held. Runtime consumers (model catalogs, the debug listener, discovery, the refresh loop) receive the same order through the runtime: each publication arrives on a per-subscriber queue with its own view and sequence, and a consumer computes from the view it holds, never from the runtime's current configuration.

Callbacks are short notifications. Expensive work belongs to the receiving component's owned execution scope. A follow-up mutation is scheduled as a later command, not as a synchronous reentry into the active commit gate. An ownership handle releases registration when the component exits, preventing strong-reference cycles and stale subscribers. Remove a callback from its list under the list lock, then release that lock before destroying its captures, because a capture can itself own another subscription.

A dispatch-time upgrade of a weak component reference stays pinned until command admission has been released, including after failed or panicking installation and initial-registration cleanup. Component teardown can then join workers that need that same admission gate without blocking dispatch against itself. Completion of a required adapter is judged against the original versioned event, not by whether the Runtime pointer has already partly advanced. While that completion is outstanding, retain the original transition input, replay the interrupted derived effects, and emit the ordinary success notification only after every required installer completes.

Function pointers and closures do not cross process address spaces. If a separately approved future deployment introduces a sidecar, its transport exchanges serialized messages and invokes callbacks locally. Thread-safe structures are not a cross-process sharing protocol.

## Explicit execution and lifetime

| Work | Default placement | Required ownership |
|---|---|---|
| Network and notification waits | In-process asynchronous tasks | Cancellation and completion tracking |
| Shared configuration queries/commands | Thread-safe in-process service | Immutable reads and serialized writes |
| Blocking encoding and filesystem work | Owned blocking tasks/threads | Admission bounds and completion before cleanup |
| External tools or justified isolation | Child processes | Explicit input/output, cancellation policy and wait/reap |

Unify ownership and shutdown rules, not every execution type into one global queue. Reuse the existing runtime, task and locking primitives.

Every background task, thread, process and subscription has a component owner. Stopping admission and asking for cancellation are not equivalent to waiting for termination. Preserve critical grants and commits, collect their outcomes, and join owned work before deleting files that work can still write. No detached task may outlive its owner and recreate a removed directory.

The local service closes management admission and new credential-preparation admission before it drains pprof and discovery. It stops further credential-notification scheduling, closes established HTTP and RESP connections as well as the accept loop, then joins admitted management work and the current blocking reconciliation. Upgraded Responses, realtime and relay sessions receive the same explicit listener cancellation and tracker context; a completed HTTP upgrade does not detach the session or its relay child. Each admitted request-log record keeps its owner token through bounded spooling and final delivery, so dropping a response cannot leave a later writer outside the shutdown wait. Runtime settlement then joins refresh work, blocking admission, render and commit jobs, and queued normalization. It drains and joins the cooldown thread outside its registry lock, and attempts pending owner publication. An unresolved publication produces an incomplete-shutdown error and retains its checkpoint; it is not converted to a successful stop.

A notification task must wait for its current blocking reload; aborting only the async waiter is insufficient. Likewise, a refresh past its scheduling wait bound keeps both its reservation and its completion ownership; freeing its scheduling slot is not permission to detach it. Implementing either rule imposes no global blocking-pool limit.

## Recovery and temporary storage

The same component acquires exclusive ownership, creates the private workspace, loads state and removes disposable work after orderly shutdown.

- **Resume:** a valid, initialized, surviving checkpoint wins. Decode it, establish memory and republish before readiness.
- **Cold start:** no recoverable checkpoint exists. Load the persistent snapshot and establish new memory and working state. Supported legacy import is a bootstrap path, not a file watcher.
- **Unusable work:** after exclusive native ownership is acquired, empty, schema-empty or corrupt work must not replace a good snapshot with empty data. Stale PID markers are not another ownership authority. Check supported schema bounds before treating unknown tables as disposable: a valid newer unsupported schema is an error, not permission to erase it.
- **Shutdown:** stop admission, settle critical persistence, cancel and join remaining owned work, and release callbacks. Clean the workspace only if no unresolved recovery checkpoint remains; otherwise report incomplete cleanup and preserve the workspace for resume before releasing ownership.

A tmpfs can disappear on power loss. Resume is possible only if recoverable work survives; otherwise use the durable published file. Stable ownership-lock identity must not be removed beneath a waiting process, and is not part of the recyclable checkpoint or portable backup. Resolve it from the canonical published target, independently of TMPDIR or an explicit working root, and hold it until cleanup finishes. The implementation reuses the existing native file-lock helper; generated lock files are not restore inputs.

That per-database guard is the only cross-process exclusion. No shared lock or coordination database spans the databases of one temporary namespace, so publication of one database never waits for another. Inside the owner, replacing the working checkpoint and holding a test-only working connection exclude each other through a plain per-database mutex taken by the single commit sequence. That mutex is not reentrant: never open a working connection, or start another publication, while one is held on the same thread.

A deliberate older-backup restore stops all owners and prevents stale working state from resuming before the published file is replaced. Replacing the file while the service runs is not a configuration API. No periodic hash, metadata, SQLite or filesystem-event reconciliation repairs bypass writes.

## What a refactor must demonstrate

Names and code organization may evolve; these behavioral checks must continue to hold:

- Copy only the published database during overlapping writes, then cold-load it in a clean location with no companion files.
- Query accepted state while persistence is slow or unavailable, without opening SQLite.
- Reject invalid, stale and dry-run changes without publishing them; preserve data after publication failure and retry or resume. Specifically test token grant -> checkpoint -> failed publication -> unrelated mutation attempt -> resolved publication or recovery, including shutdown and another already-in-flight grant. Retainable required grant data must not disappear; unretainable completion failures follow the accepted decision and must not wedge unrelated writes.
- Round-trip complete state without an old database, including raw credentials, opaque data, IDs and history.
- Notify in order with supplied views, handle registration races and release subscriptions with their owners.
- Reject stale relevant job results and keep isolated computations independent of ambient state.
- Refuse competing online and offline owners and wait for writers before workspace deletion.
- Preserve legacy WAL import, privacy, cold and resume behavior, and the boundary between configuration and credential restore.
- Meet unchanged idle and resource budgets; do not reintroduce a poller or raise a ceiling as a substitute for ownership.

These are review criteria, not assertions that the current implementation already passes them. Future implementation work must provide evidence on its final tree.
