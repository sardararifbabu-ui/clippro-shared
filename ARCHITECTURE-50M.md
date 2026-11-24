# ClipPro shared backend — architecture for 50 million users (5 crore)

Arif's requirement (2026-11-24, voice): *"যেটা করবি প্রথম থেকে করবি। এখানে মনে কর পাঁচ কোটি মানুষ এড হবে। পাঁচ কোটি মানুষ এড হলে পাঁচ কোটি এখন বসিয়ে দে।"*
— build it from the start for 50 million users joining. This document is the
design for that. It is honest about what exists today and what real
infrastructure 50M users actually needs.

## 1. What exists today (verified reference implementation)

- **One Java process** (`ClipProServer.java`, ~6,300 lines) on this VM,
  listening on `127.0.0.1:8080`, data in `~/workspace/clippro-server/data/`.
- **Store:** in-memory lists persisted as JSON files on local disk
  (`accounts.json`, `posts.json`, `likes.json`, `comments.json`,
  `follows.json`, `notifications.json`, `watches.json`, ...).
- **Media:** uploaded files on local disk under `media/`, served by the same
  process with HTTP Range support.
- **API contract:** 110 actions over `POST /actions`
  (`{"action","args"}` → `{"data":...}`), identical to the web app's server,
  so the same client works against either.
- **Verified 2026-11-24:** two-client test, 14/14 checks passed — account
  created on "phone A" logs in on "phone B", a post from A appears in B's
  For You feed, B's like/comment/follow sync back to A, @clippro admin signs
  in, media bytes + Range + cross-origin absolute URLs work.
- **APK side:** the APK's loopback server gained a remote-first proxy
  (`forwardActions` / `forwardMedia`, circuit breaker after 3 failures,
  local store as fallback) and `MainActivity` gained a `SHARED_SERVER_URL`
  constant + a startup probe. The APK builds cleanly. The proxy path is
  compile-verified and protocol-verified; it could **not** be verified
  end-to-end in this sandbox because the sandbox blocks outbound TCP from
  Java processes (see §7) — it must be confirmed on a real phone.

### Honest capacity of today's build

- Every write calls `saveAll()`, which is `synchronized` and rewrites
  **every** JSON table file on **every** mutation. Write cost grows with the
  size of the whole database, and writes serialize on one lock.
- Single JVM, single disk, no replication, no cache, no CDN, no load
  balancing, no failover. If this VM restarts, the server is down until
  someone restarts it (a watchdog script is provided, §6).
- Realistic capacity: **tens to a few hundred concurrent users, a few
  thousand accounts with light use** — fine for Arif testing with friends,
  **not** for lakhs, and **not** for 50 million. This is a correct reference
  implementation of the API, not a production platform.

## 2. Target architecture (designed for 50M from day one)

```
                    ┌────────────────────────────┐
                    │  CDN (video + images +     │
                    │  static app assets)        │
                    └──────▲───────────────▲─────┘
                           │               │
   Android APK ─┐          │               │   Web app ─┐
   iOS app ─────┤──► API gateway / load balancer ───────┤
   (future) ────┘   (TLS, rate limiting, WAF, auth)     │
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        Stateless API tier (auto-scaled, many replicas)
        (the SAME 110-action contract as ClipProServer)
              │            │            │
     ┌────────┴───┐  ┌─────┴────┐  ┌────┴──────┐
     ▼            ▼  ▼          ▼  ▼           ▼
  Sharded DB   Cache     Object      Transcode   Search /
  (primary +   (Redis)   storage     queue +     recommendations
  read         (feeds,   (S3 /      workers     (Kafka event
  replicas)    sessions, (videos,    (ffmpeg,    stream, feature
  shard by     counters) photos)    thumbnails, store, ranking
  user-id hash)           + CDN      renditions)  service)
     │            │          │          │           │
     └────────────┴──────────┴──────────┴───────────┘
              Backups, metrics, logs, alerts (24/7)
```

### Principles (these are what "from the start" actually means)

1. **Stateless API servers.** No per-server state: any API replica can serve
   any request. Scale = add replicas behind the load balancer. The current
   Java server's in-memory lists are the thing that must go — state moves to
   the database, cache and object storage.
2. **Shard the data from day one.** Every row carries its shard key:
   `accountId` for accounts, posts, likes, comments, follows, messages,
   watches. Shard by `hash(accountId) mod N`. A user's data lives on one
   shard, so reads/writes for one user never fan out. N grows by splitting
   shards — no rewrite of the app, only a rebalancing job.
3. **Never store video in the database or on an app server disk.** Uploads
   go straight to object storage (S3-compatible) via signed URLs; the API
   only stores the object key. Delivery is always from the CDN, never from
   the API servers.
4. **Video is transcoded asynchronously.** Upload → queue (SQS/Kafka) →
   worker pool transcodes to 360p/720p/1080p + thumbnails + preview, stores
   renditions in object storage, marks the post ready. The uploader never
   waits for transcoding.
5. **Feeds are precomputed + cached.** For You is not a live database scan:
   a ranking service consumes the watch-event stream, scores candidates,
   and writes per-user feed caches (Redis). The API serves feeds from cache
   and falls back to a simple chronological query when the cache is cold.
6. **Read replicas + cache for counters.** Like/comment/follower counts are
   cached and updated asynchronously; exact counts come from the primary
   only for the post owner/admin views.
7. **Everything observable and backed up.** Metrics, logs, alerts, daily
   database backups + point-in-time recovery, multi-AZ deployment.

### Data model (sharded, same logical tables as today)

| Table | Shard key | Notes |
|---|---|---|
| accounts | `hash(accountId)` | username/phone/email unique indexes via a lookup service |
| posts | `hash(accountId)` | media = object-storage key only; status: uploading/ready |
| likes / comments / follows / saves / reposts | `hash(accountId)` of the actor | counters cached in Redis |
| messages / conversations | `hash(participantId)` | inbox per user |
| watches | `hash(accountId)` | append-only event stream → Kafka → ranking |
| notifications | `hash(accountId)` | per-user inbox, push via FCM/APNs |
| reports / moderation / admin audit | `hash(targetId)` | admin reads across shards via a search index |

### Services to build (each small, independently scalable)

- **API tier** — the 110-action contract, stateless, auto-scaled.
- **Upload service** — signed-URL issue, size/type checks (25 MB/file cap
  stays), virus scan, queue enqueue.
- **Transcode workers** — ffmpeg pool, queue-driven, scales with backlog.
- **Feed/ranking service** — candidate generation + scoring from the
  watch-event stream.
- **Search service** — accounts, hashtags, sounds (indexed, not DB scans).
- **Notification/push service** — FCM/APNs, broadcast (admin → all users)
  via fan-out queue, not one giant query.
- **Moderation service** — reports, copyright takedown queue, admin tools.
- **Auth service** — phone/email/username + OTP (SMS provider), 2FA,
  OAuth (Google/Apple) via Arif's own developer accounts.

## 3. Migration path from today's build (no rewrite of the app)

- **Phase 1 — now (done):** single-node reference server + APK proxy.
  Proves the API contract and the sharing behaviour with real clients.
- **Phase 2 — production starter (thousands of users):** managed Postgres
  (primary + 1 read replica), object storage + CDN, 2–3 API servers behind
  a load balancer, managed Redis, daily backups. Same API contract; the
  Java server is replaced by the same contract on this stack. OTP via an
  SMS provider; Google/Apple OAuth via Arif's developer accounts.
- **Phase 3 — growth (100k–1M users):** shard Postgres (or move to a
  sharded store such as CockroachDB/Cassandra), transcode queue + workers,
  feed precomputation, read-replica scaling, search service.
- **Phase 4 — 50M:** multi-region, CDN with negotiated rates, dedicated
  ranking/ML infra, abuse/moderation team tooling, 24/7 on-call.
  The app (APK/web) does **not** change at any phase — only the URL it
  points at changes.

## 4. What 50M users really costs (honest, order-of-magnitude)

Video apps are the most expensive kind of app to run: storage, transcoding
and bandwidth dominate. These are planning figures, not quotes — the real
number depends on how much video each user watches/uploads.

| Scale | Indicative infra cost / month | What it buys |
|---|---|---|
| Testing with friends (today) | $0 (this VM) | Single node, no guarantees |
| Production starter (1k–10k users) | ~$200–$600 | Managed DB + replica, object storage, CDN, 2–3 API servers, Redis, backups |
| ~1M monthly users | ~$3,000–$15,000 | Sharded DB, transcode workers, feed service, bigger CDN bill |
| ~50M monthly users | ~$100,000+/month, realistically $1M+/month at TikTok-like usage | Multi-region, negotiated CDN, dedicated ranking/ML, moderation, 24/7 ops |

For scale: TikTok-scale video delivery is measured in billions of dollars a
year. **Nobody provisions literal 50-million-user hardware on day one with
zero users** — it would sit idle and burn money. The correct way to honour
"build it for 50M from the start" is what §2 does: a design that scales
horizontally (sharding, stateless API, object storage, CDN, queues) so
capacity grows with users by adding machines, not by rewriting the app.
What must be provisioned on day one is the **managed cloud account, the
domain, and the scale-ready design** — the hardware then scales with demand
via auto-scaling.

## 5. What is needed to actually run this (not available in this VM)

1. **A real cloud account in Arif's name** (AWS / Google Cloud / similar)
   with a payment method. This VM has no public IP, no inbound ports, and
   no cloud credentials.
2. **A domain name** for the API + CDN (e.g. `api.clippro.app`).
3. **Managed services:** Postgres, Redis, S3-compatible storage, CDN,
   queue, FCM push, SMS provider for OTP.
4. **Play Console account ($25 one-time)** for the Play Store release —
   explicitly out of scope for this task, but required before real users
   can install from the Store.
5. **Google/Apple developer accounts** for real Google/Apple sign-in
   (currently demo in the app) and an SMS provider account for real OTP
   (currently shown inside the app for testing).

## 6. Running the reference server (this VM)

- Start: `cd ~/workspace/clippro-server && nohup ~/workspace/android-build/jdk17/bin/java -cp classes com.clippro.app.SharedMain 8080 data ~/workspace/android-build/ClipPro/app/src/main/assets > backend.log 2>&1 &`
- Watchdog: `~/workspace/clippro-server/watchdog.sh` — restarts the backend
  if `GET /clippro-shared` stops answering. Intended to run every 5 minutes
  from cron (not yet wired — see the final report).
- Health: `GET /clippro-shared` → `{"mode":"shared",...}`.
- Verify sharing: `python3 ~/workspace/clippro-server/test_two_clients.py http://127.0.0.1:8080`
- Files: `~/workspace/clippro-server/` (server, tests, logs, data).
  APK sources: `~/workspace/android-build/ClipPro/` (proxy in
  `ClipProServer.java`, URL constant in `MainActivity.java`).

## 7. Known limitations (do not hide these)

- **No public URL yet.** A free Cloudflare quick tunnel failed: the VM's
  egress proxy blocks `api.trycloudflare.com` (HTTP 403). The backend is
  reachable only inside this VM until a real host/domain is provisioned.
- **Sandbox Java networking.** Java processes in this VM cannot make
  outbound TCP connections (the sandbox intercepts them: "Other TCP
  connections is turned off for this assistant"). That is why the APK's
  proxy path could not be verified end-to-end here — the backend itself is
  fully verified via Python/curl clients, and the proxy code compiles and
  speaks the same HTTP contract. On a real phone there is no such sandbox
  and the proxy will use the phone's normal network.
- **Current store does not scale** (§1) — it is the reference implementation
  for the API contract, and §2/§3 describe its replacement.
- **Web app and phone app do not share data yet.** The web artifact has its
  own hosted store; phones will share the new backend. Unifying them means
  repointing the web client at the shared backend once it has a public URL.
