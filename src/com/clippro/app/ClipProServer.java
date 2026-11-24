package com.clippro.app;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * ClipPro on-phone backend.
 *
 * Serves the bundled ClipPro web client from a loopback HTTP server and
 * answers the same ./actions calls the hosted space server answers, against
 * a local JSON-file store (accounts, posts, likes, comments, follows,
 * notifications, settings) with media bytes stored as files under the app's
 * private data dir. Everything the phone user creates stays on the phone:
 * there is no hosted ClipPro server to sync with, so accounts and posts made
 * in the APK are visible only in the APK, not in the hosted web app (and
 * vice versa).
 *
 * HTTP contract (mirrors the space SDK client):
 *   POST /actions  body {"action": "...", "args": {...}, "actionCallId": "..."}
 *                  -> 200 {"data": {...}}  or  400 {"error": "..."}
 *   GET  /media/<key>  -> media bytes (Range requests supported for video)
 *   GET  / , /index.html, /assets/* -> bundled client files
 */
public final class ClipProServer {

    /** Reads a bundled file, e.g. "www/index.html". Null if missing. */
    public interface AssetSource {
        byte[] read(String path);
    }

    private static final class ApiException extends Exception {
        ApiException(String message) {
            super(message);
        }
    }

    private static final String[] CATEGORIES = {
            "Comedy", "Dance", "Music", "Food", "Travel", "Pets",
            "Sports", "Fashion", "Tech", "Nature", "DIY", "Gaming"
    };

    private static final long MAX_UPLOAD_BYTES = 25_000_000L;
    private static final long MAX_BASE64_CHARS = 34_000_000L;

    private final AssetSource assets;
    private final File root;
    private final File mediaDir;
    private final Random random = new Random();

    private ServerSocket server;
    private Thread acceptThread;
    private volatile boolean running;

    // ---- shared-server (remote) mode ----------------------------------
    // When remoteBase is set, this server proxies /actions and /media to the
    // shared ClipPro backend so every phone reads and writes the SAME store,
    // and only falls back to the local on-phone store when the shared server
    // cannot be reached. The standalone shared backend itself leaves
    // remoteBase unset and sets sharedBackend instead.
    private volatile String remoteBase = null;
    private volatile boolean remoteEnabled = false;
    private volatile boolean lastRemoteOk = false;
    private volatile int remoteFailures = 0;
    private volatile boolean sharedBackend = false;
    private Thread reprobeThread;

    // ---- persisted state (all access guarded by 'this') ----
    private List<Map<String, Object>> accounts = new ArrayList<>();
    private List<Map<String, Object>> posts = new ArrayList<>();
    private List<Map<String, Object>> likes = new ArrayList<>();
    private List<Map<String, Object>> comments = new ArrayList<>();
    private List<Map<String, Object>> follows = new ArrayList<>();
    private List<Map<String, Object>> notifications = new ArrayList<>();
    private List<Map<String, Object>> blocks = new ArrayList<>();
    private List<Map<String, Object>> supportThreads = new ArrayList<>();
    private List<Map<String, Object>> supportMessages = new ArrayList<>();
    // Watch events (recordWatch): one row per viewing session, used by the
    // For You ranking. Kept separate from viewCount, which recordView owns,
    // so a watch is never counted twice.
    private List<Map<String, Object>> watches = new ArrayList<>();

    // v1.5 stores: every list the shipped client can read or write.
    private List<Map<String, Object>> saves = new ArrayList<>();
    private List<Map<String, Object>> reposts = new ArrayList<>();
    private List<Map<String, Object>> hiddenPosts = new ArrayList<>();
    private List<Map<String, Object>> mutes = new ArrayList<>();
    private List<Map<String, Object>> commentLikes = new ArrayList<>();
    private List<Map<String, Object>> reports = new ArrayList<>();
    private List<Map<String, Object>> stories = new ArrayList<>();
    private List<Map<String, Object>> storyLikes = new ArrayList<>();
    private List<Map<String, Object>> storyViews = new ArrayList<>();
    private List<Map<String, Object>> conversations = new ArrayList<>();
    private List<Map<String, Object>> messages = new ArrayList<>();
    private List<Map<String, Object>> groups = new ArrayList<>();
    private List<Map<String, Object>> groupMembers = new ArrayList<>();
    private List<Map<String, Object>> groupMessages = new ArrayList<>();
    private List<Map<String, Object>> collections = new ArrayList<>();
    private List<Map<String, Object>> collectionPosts = new ArrayList<>();
    private List<Map<String, Object>> paymentMethods = new ArrayList<>();
    private List<Map<String, Object>> paymentHistory = new ArrayList<>();
    private List<Map<String, Object>> giftTx = new ArrayList<>();
    private List<Map<String, Object>> payouts = new ArrayList<>();
    private List<Map<String, Object>> editorProjects = new ArrayList<>();
    private List<Map<String, Object>> liveSessions = new ArrayList<>();
    private List<Map<String, Object>> liveMessages = new ArrayList<>();
    private List<Map<String, Object>> liveModerators = new ArrayList<>();
    private List<Map<String, Object>> liveViewers = new ArrayList<>();
    private List<Map<String, Object>> auditLog = new ArrayList<>();
    private List<Map<String, Object>> verificationCodes = new ArrayList<>();
    private Map<String, Object> taxProfiles = new LinkedHashMap<>();
    // key = String(accountId) -> settings map
    private Map<String, Object> settings = new LinkedHashMap<>();
    private long nextAccountId = 1;
    private long nextPostId = 1;
    private long nextLikeId = 1;
    private long nextCommentId = 1;
    private long nextFollowId = 1;
    private long nextNotifId = 1;
    private long nextBlockId = 1;
    private long nextThreadId = 1;
    private long nextMessageId = 1;
    private long nextWatchId = 1;
    private long lastTsMs = 0;

    public ClipProServer(AssetSource assets, File dataDir) {
        this.assets = assets;
        this.root = new File(dataDir, "clippro");
        this.mediaDir = new File(root, "media");
        if (!root.mkdirs() && !root.isDirectory()) {
            throw new IllegalStateException("cannot create data dir: " + root);
        }
        if (!mediaDir.mkdirs() && !mediaDir.isDirectory()) {
            throw new IllegalStateException("cannot create media dir: " + mediaDir);
        }
        loadAll();
        seedIfEmpty();
    }

    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    public synchronized void start() throws IOException {
        start(0);
    }

    /** Starts the server on a fixed port (0 = any free port). Binds to
     *  127.0.0.1 by default; set the BIND_HOST env var (e.g. 0.0.0.0)
     *  when the server must be reachable from outside this machine
     *  (e.g. on a cloud host like Render). */
    public synchronized void start(int port) throws IOException {
        if (running) {
            return;
        }
        String bindHost = System.getenv("BIND_HOST");
        if (bindHost == null || bindHost.isEmpty()) {
            bindHost = "127.0.0.1";
        }
        server = new ServerSocket(port, 50, InetAddress.getByName(bindHost));
        running = true;
        acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket s = server.accept();
                    Thread t = new Thread(() -> handle(s));
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    if (running) {
                        // accept failed; keep looping
                    }
                }
            }
        });
        acceptThread.setDaemon(true);
        acceptThread.start();
        if (remoteBase != null) {
            // Background re-probe: if the shared server was unreachable at
            // startup (or the circuit breaker tripped), keep checking once a
            // minute so sharing comes back on its own. Never blocks the UI.
            reprobeThread = new Thread(() -> {
                while (running) {
                    if (remoteBase != null && !remoteEnabled) {
                        if (probeRemote()) {
                            remoteEnabled = true;
                            remoteFailures = 0;
                        }
                    }
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            reprobeThread.setDaemon(true);
            reprobeThread.start();
        }
    }

    /** Sets the shared-backend base URL (e.g. https://xxx.trycloudflare.com). */
    public void setRemoteBase(String url) {
        remoteBase = (url == null || url.isEmpty()) ? null : url.replaceAll("/+$", "");
        remoteEnabled = false;
        remoteFailures = 0;
        lastRemoteOk = false;
    }

    public void setRemoteEnabled(boolean v) {
        remoteEnabled = v;
    }

    /** Marks this instance as the shared backend itself (no proxying). */
    public void setSharedBackend(boolean v) {
        sharedBackend = v;
    }

    public String getRemoteBase() {
        return remoteBase;
    }

    public String getMode() {
        if (sharedBackend) {
            return "shared";
        }
        if (remoteBase == null) {
            return "local";
        }
        return remoteEnabled ? "remote" : "local-offline";
    }

    /** Cheap liveness probe of the shared backend. Never throws. */
    public boolean probeRemote() {
        if (remoteBase == null) {
            return false;
        }
        HttpURLConnection c = null;
        try {
            URL u = new URL(remoteBase + "/clippro-shared");
            c = (HttpURLConnection) u.openConnection();
            c.setConnectTimeout(4000);
            c.setReadTimeout(4000);
            c.setRequestMethod("GET");
            int code = c.getResponseCode();
            lastRemoteOk = code == 200;
            return lastRemoteOk;
        } catch (Exception e) {
            lastRemoteOk = false;
            return false;
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }
    }

    /** Forwards one action call to the shared backend, relaying status+body. */
    private RemoteResult forwardActions(byte[] body) throws IOException {
        HttpURLConnection c = null;
        try {
            URL u = new URL(remoteBase + "/actions");
            c = (HttpURLConnection) u.openConnection();
            c.setConnectTimeout(4000);
            // Media uploads are big base64 bodies; give them longer.
            c.setReadTimeout(body.length > 1_000_000 ? 60000 : 15000);
            c.setRequestMethod("POST");
            c.setRequestProperty("Content-Type", "application/json");
            c.setDoOutput(true);
            c.setFixedLengthStreamingMode(body.length);
            try (OutputStream os = c.getOutputStream()) {
                os.write(body);
            }
            int code = c.getResponseCode();
            InputStream is = code >= 400 ? c.getErrorStream() : c.getInputStream();
            byte[] respBody = is == null ? new byte[0] : readAll(is);
            lastRemoteOk = true;
            remoteFailures = 0;
            return new RemoteResult(code, respBody);
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }
    }

    /** Forwards one media fetch to the shared backend (Range forwarded). */
    private RemoteResult forwardMedia(String key, String range) throws IOException {
        HttpURLConnection c = null;
        try {
            URL u = new URL(remoteBase + "/media/" + key);
            c = (HttpURLConnection) u.openConnection();
            c.setConnectTimeout(4000);
            c.setReadTimeout(30000);
            c.setRequestMethod("GET");
            if (range != null) {
                c.setRequestProperty("Range", range);
            }
            int code = c.getResponseCode();
            InputStream is = code >= 400 ? c.getErrorStream() : c.getInputStream();
            byte[] respBody = is == null ? new byte[0] : readAll(is);
            RemoteResult r = new RemoteResult(code, respBody);
            r.contentRange = c.getHeaderField("Content-Range");
            r.contentType = c.getContentType();
            if (code == 200 || code == 206) {
                lastRemoteOk = true;
                remoteFailures = 0;
            }
            return r;
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }
    }

    /** Records a failed remote attempt; trips the circuit breaker at 3. */
    private void noteRemoteFailure() {
        lastRemoteOk = false;
        remoteFailures++;
        if (remoteFailures >= 3) {
            remoteEnabled = false; // stop paying the timeout on every call
        }
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] tmp = new byte[8192];
        int n;
        while ((n = in.read(tmp)) != -1) {
            buf.write(tmp, 0, n);
        }
        return buf.toByteArray();
    }

    /** Rewrites relative /media/ URLs to absolute ones for cross-origin web callers. */
    private static String absolutizeMediaUrls(String json, String host) {
        if (host == null || host.isEmpty()) {
            return json;
        }
        return json.replace("\"/media/", "\"https://" + host + "/media/");
    }

    private static Map<String, String> corsHeaders() {
        Map<String, String> extra = new LinkedHashMap<>();
        extra.put("Access-Control-Allow-Origin", "*");
        return extra;
    }

    private static final class RemoteResult {
        final int status;
        final byte[] body;
        String contentRange;
        String contentType;

        RemoteResult(int status, byte[] body) {
            this.status = status;
            this.body = body;
        }
    }

    public int getPort() {
        return server == null ? -1 : server.getLocalPort();
    }

    public synchronized void stop() {
        running = false;
        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException ignored) {
        }
    }

    // ------------------------------------------------------------------
    // persistence
    // ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private synchronized void loadAll() {
        accounts = loadList("accounts.json");
        posts = loadList("posts.json");
        likes = loadList("likes.json");
        comments = loadList("comments.json");
        follows = loadList("follows.json");
        notifications = loadList("notifications.json");
        blocks = loadList("blocks.json");
        supportThreads = loadList("support_threads.json");
        supportMessages = loadList("support_messages.json");
        watches = loadList("watches.json");
        saves = loadList("saves.json");
        reposts = loadList("reposts.json");
        hiddenPosts = loadList("hidden_posts.json");
        mutes = loadList("mutes.json");
        commentLikes = loadList("comment_likes.json");
        reports = loadList("reports.json");
        stories = loadList("stories.json");
        storyLikes = loadList("story_likes.json");
        storyViews = loadList("story_views.json");
        conversations = loadList("conversations.json");
        messages = loadList("messages.json");
        groups = loadList("groups.json");
        groupMembers = loadList("group_members.json");
        groupMessages = loadList("group_messages.json");
        collections = loadList("collections.json");
        collectionPosts = loadList("collection_posts.json");
        paymentMethods = loadList("payment_methods.json");
        paymentHistory = loadList("payment_history.json");
        giftTx = loadList("gift_tx.json");
        payouts = loadList("payouts.json");
        editorProjects = loadList("editor_projects.json");
        liveSessions = loadList("live_sessions.json");
        liveMessages = loadList("live_messages.json");
        liveModerators = loadList("live_moderators.json");
        liveViewers = loadList("live_viewers.json");
        auditLog = loadList("audit_log.json");
        verificationCodes = loadList("verification_codes.json");
        Object tax = loadDoc("tax.json");
        taxProfiles = (tax instanceof Map) ? (Map<String, Object>) tax : new LinkedHashMap<>();
        Object s = loadDoc("settings.json");
        settings = (s instanceof Map) ? (Map<String, Object>) s : new LinkedHashMap<>();
        Object meta = loadDoc("meta.json");
        if (meta instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) meta;
            nextAccountId = num(m.get("nextAccountId"), 1);
            nextPostId = num(m.get("nextPostId"), 1);
            nextLikeId = num(m.get("nextLikeId"), 1);
            nextCommentId = num(m.get("nextCommentId"), 1);
            nextFollowId = num(m.get("nextFollowId"), 1);
            nextNotifId = num(m.get("nextNotifId"), 1);
            nextBlockId = num(m.get("nextBlockId"), 1);
            nextThreadId = num(m.get("nextThreadId"), 1);
            nextMessageId = num(m.get("nextMessageId"), 1);
            nextWatchId = num(m.get("nextWatchId"), 1);
        }
        // repair next ids from existing rows
        nextAccountId = Math.max(nextAccountId, maxId(accounts) + 1);
        nextPostId = Math.max(nextPostId, maxId(posts) + 1);
        nextLikeId = Math.max(nextLikeId, maxId(likes) + 1);
        nextCommentId = Math.max(nextCommentId, maxId(comments) + 1);
        nextFollowId = Math.max(nextFollowId, maxId(follows) + 1);
        nextNotifId = Math.max(nextNotifId, maxId(notifications) + 1);
        nextBlockId = Math.max(nextBlockId, maxId(blocks) + 1);
        nextThreadId = Math.max(nextThreadId, maxId(supportThreads) + 1);
        nextMessageId = Math.max(nextMessageId, maxId(supportMessages) + 1);
        nextWatchId = Math.max(nextWatchId, maxId(watches) + 1);
        // v1.0 -> v1.1 migration: accounts created by v1.0 have no admin /
        // profile-detail fields. The official @clippro account is the admin.
        for (Map<String, Object> a : accounts) {
            if (!a.containsKey("isAdmin")) {
                a.put("isAdmin", "clippro".equals(str(a.get("username"), "")) ? 1L : 0L);
            }
            if (!a.containsKey("dateOfBirth")) {
                a.put("dateOfBirth", null);
            }
            if (!a.containsKey("region")) {
                a.put("region", "");
            }
            // v1.5 migration: wallet, password, verification, suspension.
            if (!a.containsKey("password")) {
                a.put("password", null);
            }
            if (!a.containsKey("emailVerified")) {
                a.put("emailVerified", 0L);
            }
            if (!a.containsKey("phoneVerified")) {
                a.put("phoneVerified", 0L);
            }
            if (!a.containsKey("twoFactorEnabled")) {
                a.put("twoFactorEnabled", 0L);
            }
            if (!a.containsKey("suspended")) {
                a.put("suspended", 0L);
            }
            if (!a.containsKey("coins")) {
                a.put("coins", 0L);
            }
            if (!a.containsKey("diamonds")) {
                a.put("diamonds", 0L);
            }
        }
    }

    private long maxId(List<Map<String, Object>> rows) {
        long m = 0;
        for (Map<String, Object> r : rows) {
            m = Math.max(m, num(r.get("id"), 0));
        }
        return m;
    }

    private synchronized void saveAll() {
        saveDoc("accounts.json", accounts);
        saveDoc("posts.json", posts);
        saveDoc("likes.json", likes);
        saveDoc("comments.json", comments);
        saveDoc("follows.json", follows);
        saveDoc("notifications.json", notifications);
        saveDoc("blocks.json", blocks);
        saveDoc("support_threads.json", supportThreads);
        saveDoc("support_messages.json", supportMessages);
        saveDoc("watches.json", watches);
        saveDoc("saves.json", saves);
        saveDoc("reposts.json", reposts);
        saveDoc("hidden_posts.json", hiddenPosts);
        saveDoc("mutes.json", mutes);
        saveDoc("comment_likes.json", commentLikes);
        saveDoc("reports.json", reports);
        saveDoc("stories.json", stories);
        saveDoc("story_likes.json", storyLikes);
        saveDoc("story_views.json", storyViews);
        saveDoc("conversations.json", conversations);
        saveDoc("messages.json", messages);
        saveDoc("groups.json", groups);
        saveDoc("group_members.json", groupMembers);
        saveDoc("group_messages.json", groupMessages);
        saveDoc("collections.json", collections);
        saveDoc("collection_posts.json", collectionPosts);
        saveDoc("payment_methods.json", paymentMethods);
        saveDoc("payment_history.json", paymentHistory);
        saveDoc("gift_tx.json", giftTx);
        saveDoc("payouts.json", payouts);
        saveDoc("editor_projects.json", editorProjects);
        saveDoc("live_sessions.json", liveSessions);
        saveDoc("live_messages.json", liveMessages);
        saveDoc("live_moderators.json", liveModerators);
        saveDoc("live_viewers.json", liveViewers);
        saveDoc("audit_log.json", auditLog);
        saveDoc("verification_codes.json", verificationCodes);
        saveDoc("tax.json", taxProfiles);
        saveDoc("settings.json", settings);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("nextAccountId", nextAccountId);
        meta.put("nextPostId", nextPostId);
        meta.put("nextLikeId", nextLikeId);
        meta.put("nextCommentId", nextCommentId);
        meta.put("nextFollowId", nextFollowId);
        meta.put("nextNotifId", nextNotifId);
        meta.put("nextBlockId", nextBlockId);
        meta.put("nextThreadId", nextThreadId);
        meta.put("nextMessageId", nextMessageId);
        meta.put("nextWatchId", nextWatchId);
        saveDoc("meta.json", meta);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> loadList(String name) {
        Object v = loadDoc(name);
        if (v instanceof List) {
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object o : (List<Object>) v) {
                if (o instanceof Map) {
                    out.add((Map<String, Object>) o);
                }
            }
            return out;
        }
        return new ArrayList<>();
    }

    private Object loadDoc(String name) {
        File f = new File(root, name);
        if (!f.exists()) {
            return null;
        }
        try {
            byte[] b = Files.readAllBytes(f.toPath());
            if (b.length == 0) {
                return null;
            }
            return Json.parse(new String(b, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return null;
        }
    }

    private void saveDoc(String name, Object value) {
        File tmp = new File(root, name + ".tmp");
        File dst = new File(root, name);
        try {
            Files.write(tmp.toPath(), Json.stringify(value).getBytes(StandardCharsets.UTF_8));
            if (!tmp.renameTo(dst)) {
                Files.deleteIfExists(dst.toPath());
                Files.move(tmp.toPath(), dst.toPath());
            }
        } catch (IOException ignored) {
        }
    }

    private String nowIso() {
        long ms = Math.max(System.currentTimeMillis(), lastTsMs + 1);
        lastTsMs = ms;
        return Instant.ofEpochMilli(ms).toString();
    }

    // ------------------------------------------------------------------
    // HTTP
    // ------------------------------------------------------------------

    private void handle(Socket socket) {
        try (Socket s = socket;
             InputStream in = s.getInputStream();
             OutputStream out = s.getOutputStream()) {
            String requestLine = readLine(in);
            if (requestLine == null || requestLine.isEmpty()) {
                return;
            }
            String[] parts = requestLine.split(" ");
            if (parts.length < 2) {
                return;
            }
            String method = parts[0];
            String rawPath = parts[1];
            String path = rawPath.split("\\?")[0];
            try {
                path = URLDecoder.decode(path, "UTF-8");
            } catch (Exception ignored) {
            }
            Map<String, String> headers = new LinkedHashMap<>();
            String line;
            while ((line = readLine(in)) != null && !line.isEmpty()) {
                int c = line.indexOf(':');
                if (c > 0) {
                    headers.put(line.substring(0, c).trim().toLowerCase(),
                            line.substring(c + 1).trim());
                }
            }
            int contentLength = 0;
            try {
                contentLength = Integer.parseInt(headers.getOrDefault("content-length", "0"));
            } catch (NumberFormatException ignored) {
            }
            byte[] body = new byte[0];
            if (contentLength > 0) {
                body = readExact(in, contentLength);
            }
            if ("OPTIONS".equalsIgnoreCase(method)) {
                Map<String, String> extra = new LinkedHashMap<>();
                extra.put("Access-Control-Allow-Origin", "*");
                extra.put("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
                extra.put("Access-Control-Allow-Headers", "Content-Type");
                extra.put("Access-Control-Max-Age", "86400");
                send(out, 204, "text/plain", new byte[0], extra);
            } else if ("GET".equalsIgnoreCase(method) && "/clippro-shared".equals(path)) {
                handleSharedStatus(out);
            } else if ("POST".equalsIgnoreCase(method) && "/actions".equals(path)) {
                handleActions(body, headers, out);
            } else if ("GET".equalsIgnoreCase(method) && path.startsWith("/media/")) {
                handleMedia(path.substring("/media/".length()), headers, out);
            } else if ("GET".equalsIgnoreCase(method)) {
                handleStatic(path, out);
            } else {
                send(out, 405, "text/plain", "Method not allowed".getBytes(StandardCharsets.UTF_8), null);
            }
        } catch (Exception ignored) {
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                buf.write(b);
            }
        }
        if (b == -1 && buf.size() == 0) {
            return null;
        }
        return buf.toString("UTF-8");
    }

    private static byte[] readExact(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int r = in.read(buf, off, n - off);
            if (r == -1) {
                break;
            }
            off += r;
        }
        if (off < n) {
            byte[] cut = new byte[off];
            System.arraycopy(buf, 0, cut, 0, off);
            return cut;
        }
        return buf;
    }

    private void handleStatic(String path, OutputStream out) throws IOException {
        String assetPath;
        if ("/".equals(path) || "".equals(path)) {
            assetPath = "www/index.html";
        } else if (path.startsWith("/assets/")) {
            assetPath = "www" + path;
        } else {
            assetPath = "www" + path;
        }
        byte[] data = assets.read(assetPath);
        if (data == null) {
            send(out, 404, "text/plain", "Not found".getBytes(StandardCharsets.UTF_8), null);
            return;
        }
        send(out, 200, contentType(assetPath), data, null);
    }

    private void handleMedia(String key, Map<String, String> headers, OutputStream out) throws IOException {
        if (key.contains("..") || key.startsWith("/") || key.isEmpty()) {
            send(out, 400, "text/plain", "Bad key".getBytes(StandardCharsets.UTF_8), null);
            return;
        }
        // Remote-first: shared media lives on the shared backend.
        if (remoteBase != null && remoteEnabled) {
            try {
                RemoteResult r = forwardMedia(key, headers.get("range"));
                if (r.status == 200 || r.status == 206) {
                    Map<String, String> extra = new LinkedHashMap<>();
                    extra.put("Accept-Ranges", "bytes");
                    extra.put("Access-Control-Allow-Origin", "*");
                    if (r.status == 206 && r.contentRange != null) {
                        extra.put("Content-Range", r.contentRange);
                    }
                    String type = r.contentType != null ? r.contentType : contentType(key);
                    send(out, r.status, type, r.body, extra);
                    return;
                }
                // 404 on the shared server: the media may be an older
                // on-phone-only file, so fall through to the local file.
            } catch (Exception e) {
                noteRemoteFailure();
            }
        }
        File f = new File(mediaDir, key);
        if (!f.isFile()) {
            send(out, 404, "text/plain", "Not found".getBytes(StandardCharsets.UTF_8), null);
            return;
        }
        byte[] data = Files.readAllBytes(f.toPath());
        String type = contentType(key);
        String range = headers.get("range");
        if (range != null && range.startsWith("bytes=")) {
            try {
                String spec = range.substring("bytes=".length()).trim();
                String[] se = spec.split("-", 2);
                long start = se[0].isEmpty() ? 0 : Long.parseLong(se[0]);
                long end = (se.length > 1 && !se[1].isEmpty())
                        ? Long.parseLong(se[1]) : data.length - 1;
                if (start < 0) {
                    start = 0;
                }
                if (end >= data.length) {
                    end = data.length - 1;
                }
                if (start <= end) {
                    int len = (int) (end - start + 1);
                    byte[] slice = new byte[len];
                    System.arraycopy(data, (int) start, slice, 0, len);
                    Map<String, String> extra = new LinkedHashMap<>();
                    extra.put("Content-Range", "bytes " + start + "-" + end + "/" + data.length);
                    extra.put("Accept-Ranges", "bytes");
                    extra.put("Access-Control-Allow-Origin", "*");
                    send(out, 206, type, slice, extra);
                    return;
                }
            } catch (Exception ignored) {
                // fall through to full body
            }
        }
        Map<String, String> extra = new LinkedHashMap<>();
        extra.put("Accept-Ranges", "bytes");
        extra.put("Access-Control-Allow-Origin", "*");
        send(out, 200, type, data, extra);
    }

    private void handleSharedStatus(OutputStream out) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", getMode());
        m.put("remoteUrl", remoteBase);
        m.put("remoteOk", lastRemoteOk);
        m.put("server", "clippro-shared");
        send(out, 200, "application/json",
                Json.stringify(m).getBytes(StandardCharsets.UTF_8), corsHeaders());
    }

    private void handleActions(byte[] body, Map<String, String> headers, OutputStream out) throws IOException {
        // Remote-first: when the shared backend is reachable, forward the call
        // verbatim so every phone reads and writes the same shared store.
        if (remoteBase != null && remoteEnabled) {
            try {
                RemoteResult r = forwardActions(body);
                send(out, r.status, "application/json", r.body, corsHeaders());
                return;
            } catch (Exception e) {
                noteRemoteFailure();
                // fall through to the local store
            }
        }
        try {
            Object parsed = Json.parse(new String(body, StandardCharsets.UTF_8));
            if (!(parsed instanceof Map)) {
                throw new ApiException("Bad request");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> req = (Map<String, Object>) parsed;
            String action = str(req.get("action"), "");
            @SuppressWarnings("unchecked")
            Map<String, Object> args = (req.get("args") instanceof Map)
                    ? (Map<String, Object>) req.get("args") : new LinkedHashMap<>();
            Object data;
            synchronized (this) {
                data = dispatch(action, args);
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("data", data);
            String json = Json.stringify(resp);
            // Cross-origin (web) callers need absolute media URLs; the phone
            // client is same-origin and keeps relative ones.
            if (headers.get("origin") != null) {
                json = absolutizeMediaUrls(json, headers.get("host"));
            }
            send(out, 200, "application/json",
                    json.getBytes(StandardCharsets.UTF_8), corsHeaders());
        } catch (ApiException e) {
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("error", e.getMessage());
            send(out, 400, "application/json",
                    Json.stringify(resp).getBytes(StandardCharsets.UTF_8), corsHeaders());
        } catch (Exception e) {
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("error", "Server error: " + e.getMessage());
            send(out, 500, "application/json",
                    Json.stringify(resp).getBytes(StandardCharsets.UTF_8), corsHeaders());
        }
    }

    private static void send(OutputStream out, int status, String contentType,
                             byte[] body, Map<String, String> extra) throws IOException {
        String reason = status == 200 ? "OK" : status == 204 ? "No Content"
                : status == 206 ? "Partial Content"
                : status == 400 ? "Bad Request" : status == 404 ? "Not Found"
                : status == 405 ? "Method Not Allowed" : "Internal Server Error";
        StringBuilder h = new StringBuilder();
        h.append("HTTP/1.1 ").append(status).append(' ').append(reason).append("\r\n");
        h.append("Content-Type: ").append(contentType).append("\r\n");
        h.append("Content-Length: ").append(body.length).append("\r\n");
        h.append("Connection: close\r\n");
        if (extra != null) {
            for (Map.Entry<String, String> e : extra.entrySet()) {
                h.append(e.getKey()).append(": ").append(e.getValue()).append("\r\n");
            }
        }
        h.append("\r\n");
        out.write(h.toString().getBytes(StandardCharsets.UTF_8));
        out.write(body);
        out.flush();
    }

    private static String contentType(String name) {
        String n = name.toLowerCase();
        if (n.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (n.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (n.endsWith(".js")) {
            return "text/javascript; charset=utf-8";
        }
        if (n.endsWith(".json")) {
            return "application/json";
        }
        if (n.endsWith(".png")) {
            return "image/png";
        }
        if (n.endsWith(".jpg") || n.endsWith(".jpeg")) {
            return "image/jpeg";
        }
        if (n.endsWith(".webp")) {
            return "image/webp";
        }
        if (n.endsWith(".gif")) {
            return "image/gif";
        }
        if (n.endsWith(".mp4")) {
            return "video/mp4";
        }
        if (n.endsWith(".webm")) {
            return "video/webm";
        }
        if (n.endsWith(".mov")) {
            return "video/quicktime";
        }
        return "application/octet-stream";
    }

    // ------------------------------------------------------------------
    // action dispatch
    // ------------------------------------------------------------------

    private Object dispatch(String action, Map<String, Object> args) throws ApiException {
        switch (action) {
            case "createAccount": return createAccount(args);
            case "loginWithContact": return loginWithContact(args);
            case "listAccounts": return listAccounts(args);
            case "getFeed": return getFeed(args);
            case "createPost": return createPost(args);
            case "deletePost": return deletePost(args);
            case "recordView": return recordView(args);
            case "recordWatch": return recordWatch(args);
            case "toggleLike": return toggleLike(args);
            case "listComments": return listComments(args);
            case "addComment": return addComment(args);
            case "toggleFollow": return toggleFollow(args);
            case "getProfile": return getProfile(args);
            case "search": return search(args);
            case "getNotifications": return getNotifications(args);
            case "markNotificationsRead": return markNotificationsRead(args);
            case "getSettings": return getSettings(args);
            case "updateSettings": return updateSettings(args);
            case "updateProfile": return updateProfile(args);
            case "updateAccountDetails": return updateAccountDetails(args);
            case "toggleBlock": return toggleBlock(args);
            case "listBlocked": return listBlocked(args);
            case "exportMyData": return exportMyData(args);
            case "deleteMyAccount": return deleteMyAccount(args);
            case "adminOverview": return adminOverview(args);
            case "adminDeletePost": return adminDeletePost(args);
            case "adminRemoveAccount": return adminRemoveAccount(args);
            case "createSupportThread": return createSupportThread(args);
            case "listMySupportThreads": return listMySupportThreads(args);
            case "adminListSupportThreads": return adminListSupportThreads(args);
            case "getSupportThread": return getSupportThread(args);
            case "replySupportThread": return replySupportThread(args);
            case "closeSupportThread": return closeSupportThread(args);
            case "seedStarterContent": return seedStarterContent(args);
            case "toggleSave": return toggleSave(args);
            case "toggleRepost": return toggleRepost(args);
            case "hidePost": return hidePost(args);
            case "toggleMute": return toggleMute(args);
            case "sharePost": return sharePost(args);
            case "listSavedPosts": return listSavedPosts(args);
            case "toggleCommentLike": return toggleCommentLike(args);
            case "togglePinComment": return togglePinComment(args);
            case "deleteComment": return deleteComment(args);
            case "createStory": return createStory(args);
            case "deleteStory": return deleteStory(args);
            case "toggleStoryLike": return toggleStoryLike(args);
            case "listStories": return listStories(args);
            case "openConversation": return openConversation(args);
            case "listConversations": return listConversations(args);
            case "listMessages": return listMessages(args);
            case "sendMessage": return sendMessage(args);
            case "sendMediaMessage": return sendMediaMessage(args);
            case "createGroup": return createGroup(args);
            case "addGroupMember": return addGroupMember(args);
            case "listGroups": return listGroups(args);
            case "listGroupMessages": return listGroupMessages(args);
            case "sendGroupMessage": return sendGroupMessage(args);
            case "createCollection": return createCollection(args);
            case "deleteCollection": return deleteCollection(args);
            case "toggleCollectionPost": return toggleCollectionPost(args);
            case "listCollections": return listCollections(args);
            case "saveEditorProject": return saveEditorProject(args);
            case "listEditorProjects": return listEditorProjects(args);
            case "deleteEditorProject": return deleteEditorProject(args);
            case "getWallet": return getWallet(args);
            case "buyCoins": return buyCoins(args);
            case "sendGift": return sendGift(args);
            case "getEarnings": return getEarnings(args);
            case "addPaymentMethod": return addPaymentMethod(args);
            case "removePaymentMethod": return removePaymentMethod(args);
            case "setDefaultPaymentMethod": return setDefaultPaymentMethod(args);
            case "updateTaxProfile": return updateTaxProfile(args);
            case "getPaymentsOverview": return getPaymentsOverview(args);
            case "requestPayout": return requestPayout(args);
            case "startLive": return startLive(args);
            case "endLive": return endLive(args);
            case "listLive": return listLive(args);
            case "joinLive": return joinLive(args);
            case "leaveLive": return leaveLive(args);
            case "listLiveMessages": return listLiveMessages(args);
            case "sendLiveMessage": return sendLiveMessage(args);
            case "addLiveModerator": return addLiveModerator(args);
            case "removeLiveModerator": return removeLiveModerator(args);
            case "listLiveModerators": return listLiveModerators(args);
            case "createReport": return createReport(args);
            case "adminListReports": return adminListReports(args);
            case "adminResolveReport": return adminResolveReport(args);
            case "adminDeleteComment": return adminDeleteComment(args);
            case "adminSuspendAccount": return adminSuspendAccount(args);
            case "adminListAuditLog": return adminListAuditLog(args);
            case "adminListPayouts": return adminListPayouts(args);
            case "adminSetPayoutStatus": return adminSetPayoutStatus(args);
            case "adminBroadcastNotification": return adminBroadcastNotification(args);
            case "getAuthSecurity": return getAuthSecurity(args);
            case "setPassword": return setPassword(args);
            case "setTwoFactor": return setTwoFactor(args);
            case "requestVerificationCode": return requestVerificationCode(args);
            case "verifyCode": return verifyCode(args);
            case "resetPasswordWithCode": return resetPasswordWithCode(args);
            case "getOAuthStatus": return getOAuthStatus(args);
            case "sendTestPush": return sendTestPush(args);
            case "getTrendingHashtags": return getTrendingHashtags(args);
            case "getCreatorStats": return getCreatorStats(args);
            case "listFollowList": return listFollowList(args);
            case "removeFollower": return removeFollower(args);
            default: throw new ApiException("Unknown action: " + action);
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static long num(Object v, long def) {
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return def;
    }

    private static String str(Object v, String def) {
        return (v instanceof String) ? (String) v : def;
    }

    private static boolean bool(Object v, boolean def) {
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue() != 0;
        }
        return def;
    }

    private static long reqLong(Map<String, Object> args, String key) throws ApiException {
        Object v = args.get(key);
        if (!(v instanceof Number)) {
            throw new ApiException("Missing or invalid " + key);
        }
        return ((Number) v).longValue();
    }

    private static String reqString(Map<String, Object> args, String key) throws ApiException {
        Object v = args.get(key);
        if (!(v instanceof String)) {
            throw new ApiException("Missing or invalid " + key);
        }
        return (String) v;
    }

    private Map<String, Object> accountById(long id) {
        for (Map<String, Object> a : accounts) {
            if (num(a.get("id"), -1) == id) {
                return a;
            }
        }
        return null;
    }

    private Map<String, Object> postById(long id) {
        for (Map<String, Object> p : posts) {
            if (num(p.get("id"), -1) == id) {
                return p;
            }
        }
        return null;
    }

    private Map<String, Object> defaultSettings() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("autoplay", 1L);
        s.put("dataSaver", 0L);
        s.put("videoQuality", "auto");
        s.put("notifyLikes", 1L);
        s.put("notifyComments", 1L);
        s.put("notifyFollowers", 1L);
        s.put("notifyNewPosts", 1L);
        s.put("commentPermission", "everyone");
        s.put("viewPermission", "everyone");
        s.put("showLiked", 1L);
        s.put("allowDownloads", 1L);
        s.put("suggestAccount", 1L);
        s.put("restrictedMode", 0L);
        s.put("personalizedAds", 1L);
        s.put("pushEnabled", 1L);
        s.put("reduceMotion", 0L);
        s.put("textSize", "normal");
        s.put("screenTimeLimit", 0L);
        s.put("mentionsPermission", "everyone");
        s.put("dmPermission", "everyone");
        s.put("duetPermission", "everyone");
        s.put("stitchPermission", "everyone");
        s.put("notifyMentions", 1L);
        s.put("securityAlerts", 1L);
        s.put("filterKeywords", "[]");
        s.put("language", "en");
        s.put("interests", "[]");
        return s;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> settingsRow(long accountId) {
        Object v = settings.get(String.valueOf(accountId));
        if (v instanceof Map) {
            return (Map<String, Object>) v;
        }
        Map<String, Object> s = defaultSettings();
        settings.put(String.valueOf(accountId), s);
        return s;
    }

    private Map<String, Object> toAccount(Map<String, Object> row) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", num(row.get("id"), 0));
        m.put("username", str(row.get("username"), ""));
        m.put("displayName", str(row.get("displayName"), ""));
        m.put("bio", str(row.get("bio"), ""));
        m.put("isPrivate", num(row.get("isPrivate"), 0) == 1);
        m.put("isAdmin", num(row.get("isAdmin"), 0) == 1);
        m.put("dateOfBirth", row.get("dateOfBirth") instanceof String
                ? str(row.get("dateOfBirth"), "") : null);
        m.put("region", str(row.get("region"), ""));
        m.put("createdAt", str(row.get("createdAt"), ""));
        return m;
    }

    private Map<String, Object> toSettings(Map<String, Object> account) {
        Map<String, Object> s = settingsRow(num(account.get("id"), 0));
        List<Object> interests = new ArrayList<>();
        try {
            Object parsed = Json.parse(str(s.get("interests"), "[]"));
            if (parsed instanceof List) {
                for (Object x : (List<Object>) parsed) {
                    if (x instanceof String) {
                        interests.add(x);
                    }
                }
            }
        } catch (Exception ignored) {
        }
        List<Object> filterKeywords = new ArrayList<>();
        try {
            Object parsed = Json.parse(str(s.get("filterKeywords"), "[]"));
            if (parsed instanceof List) {
                for (Object x : (List<Object>) parsed) {
                    if (x instanceof String) {
                        filterKeywords.add(x);
                    }
                }
            }
        } catch (Exception ignored) {
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("autoplay", num(s.get("autoplay"), 1) == 1);
        m.put("dataSaver", num(s.get("dataSaver"), 0) == 1);
        m.put("videoQuality", str(s.get("videoQuality"), "auto"));
        m.put("notifyLikes", num(s.get("notifyLikes"), 1) == 1);
        m.put("notifyComments", num(s.get("notifyComments"), 1) == 1);
        m.put("notifyFollowers", num(s.get("notifyFollowers"), 1) == 1);
        m.put("notifyNewPosts", num(s.get("notifyNewPosts"), 1) == 1);
        m.put("commentPermission", str(s.get("commentPermission"), "everyone"));
        m.put("viewPermission", str(s.get("viewPermission"), "everyone"));
        m.put("showLiked", num(s.get("showLiked"), 1) == 1);
        m.put("allowDownloads", num(s.get("allowDownloads"), 1) == 1);
        m.put("suggestAccount", num(s.get("suggestAccount"), 1) == 1);
        m.put("restrictedMode", num(s.get("restrictedMode"), 0) == 1);
        m.put("personalizedAds", num(s.get("personalizedAds"), 1) == 1);
        m.put("pushEnabled", num(s.get("pushEnabled"), 1) == 1);
        m.put("reduceMotion", num(s.get("reduceMotion"), 0) == 1);
        m.put("textSize", str(s.get("textSize"), "normal"));
        m.put("screenTimeLimit", num(s.get("screenTimeLimit"), 0));
        m.put("mentionsPermission", str(s.get("mentionsPermission"), "everyone"));
        m.put("dmPermission", str(s.get("dmPermission"), "everyone"));
        m.put("duetPermission", str(s.get("duetPermission"), "everyone"));
        m.put("stitchPermission", str(s.get("stitchPermission"), "everyone"));
        m.put("notifyMentions", num(s.get("notifyMentions"), 1) == 1);
        m.put("securityAlerts", num(s.get("securityAlerts"), 1) == 1);
        m.put("filterKeywords", filterKeywords);
        m.put("isPrivate", num(account.get("isPrivate"), 0) == 1);
        m.put("isAdmin", num(account.get("isAdmin"), 0) == 1);
        m.put("language", str(s.get("language"), "en"));
        m.put("interests", interests);
        return m;
    }

    private boolean isFollowing(long followerId, long followeeId) {
        if (followerId == followeeId) {
            return false;
        }
        for (Map<String, Object> f : follows) {
            if (num(f.get("followerId"), -1) == followerId
                    && num(f.get("followeeId"), -1) == followeeId) {
                return true;
            }
        }
        return false;
    }

    private boolean isBlockedEitherWay(long a, long b) {
        if (a == b || a == 0 || b == 0) {
            return false;
        }
        for (Map<String, Object> bl : blocks) {
            long blocker = num(bl.get("blockerId"), -1);
            long blocked = num(bl.get("blockedId"), -1);
            if ((blocker == a && blocked == b) || (blocker == b && blocked == a)) {
                return true;
            }
        }
        return false;
    }

    private boolean canViewAuthor(long viewerId, Map<String, Object> author) {
        long authorId = num(author.get("id"), -1);
        if (viewerId == authorId) {
            return true;
        }
        if (num(author.get("suspended"), 0) == 1) {
            return false;
        }
        if (isBlockedEitherWay(viewerId, authorId)) {
            return false;
        }
        Map<String, Object> s = settingsRow(authorId);
        String perm = str(s.get("viewPermission"), "everyone");
        if ("onlyme".equals(perm)) {
            return false;
        }
        boolean followsAuthor = isFollowing(viewerId, authorId);
        if ("followers".equals(perm)) {
            return followsAuthor;
        }
        if (num(author.get("isPrivate"), 0) == 1) {
            return followsAuthor;
        }
        return true;
    }

    private void notify(long recipientId, long actorId, String type, Long postId) {
        if (recipientId == actorId) {
            return;
        }
        Map<String, Object> s = settingsRow(recipientId);
        if (num(s.get("pushEnabled"), 1) == 0) {
            return;
        }
        boolean enabled;
        switch (type) {
            case "like": enabled = num(s.get("notifyLikes"), 1) == 1; break;
            case "comment": enabled = num(s.get("notifyComments"), 1) == 1; break;
            case "follow": enabled = num(s.get("notifyFollowers"), 1) == 1; break;
            case "system": enabled = num(s.get("securityAlerts"), 1) == 1; break;
            case "support": enabled = true; break;
            default: enabled = num(s.get("notifyNewPosts"), 1) == 1; break;
        }
        if (!enabled) {
            return;
        }
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("id", nextNotifId++);
        n.put("accountId", recipientId);
        n.put("actorId", actorId);
        n.put("type", type);
        n.put("postId", postId);
        n.put("read", 0L);
        n.put("createdAt", nowIso());
        notifications.add(n);
    }

    private long countWhere(List<Map<String, Object>> rows, String key, long value) {
        long c = 0;
        for (Map<String, Object> r : rows) {
            if (num(r.get(key), -1) == value) {
                c++;
            }
        }
        return c;
    }

    private Map<String, Object> hydratePost(long viewerId, Map<String, Object> row) {
        Map<String, Object> author = accountById(num(row.get("accountId"), -1));
        if (author == null) {
            return null;
        }
        long postId = num(row.get("id"), 0);
        boolean liked = false;
        if (viewerId > 0) {
            for (Map<String, Object> l : likes) {
                if (num(l.get("postId"), -1) == postId
                        && num(l.get("accountId"), -1) == viewerId) {
                    liked = true;
                    break;
                }
            }
        }
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", num(author.get("id"), 0));
        a.put("username", str(author.get("username"), ""));
        a.put("displayName", str(author.get("displayName"), ""));
        a.put("isPrivate", num(author.get("isPrivate"), 0) == 1);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", postId);
        m.put("kind", str(row.get("kind"), ""));
        m.put("mediaUrl", "/media/" + str(row.get("mediaBlobKey"), ""));
        m.put("caption", str(row.get("caption"), ""));
        m.put("category", str(row.get("category"), ""));
        m.put("viewCount", num(row.get("viewCount"), 0));
        m.put("likeCount", countWhere(likes, "postId", postId));
        m.put("commentCount", countWhere(comments, "postId", postId));
        // shareCount was missing here, so the client rendered "undefined"
        // under the share arrow. Default to 0 when the post has none.
        m.put("shareCount", num(row.get("shareCount"), 0));
        m.put("liked", liked);
        m.put("saved", viewerId > 0 && isSaved(viewerId, postId));
        m.put("location", str(row.get("location"), ""));
        m.put("commentsOff", num(row.get("commentsOff"), 0) == 1);
        m.put("duetOf", null);
        m.put("stitchOf", null);
        m.put("downloadsAllowed",
                num(settingsRow(num(author.get("id"), 0)).get("allowDownloads"), 1) == 1);
        m.put("createdAt", str(row.get("createdAt"), ""));
        m.put("author", a);
        m.put("authorFollowed", viewerId > 0
                && isFollowing(viewerId, num(author.get("id"), 0)));
        return m;
    }

    private List<Map<String, Object>> hydratePosts(long viewerId,
                                                   List<Map<String, Object>> rows) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> h = hydratePost(viewerId, r);
            if (h != null) {
                out.add(h);
            }
        }
        return out;
    }

    private static boolean isCategory(String c) {
        for (String k : CATEGORIES) {
            if (k.equals(c)) {
                return true;
            }
        }
        return false;
    }

    /** @usernames in a comment, lowercased, in first-seen order, deduped. */
    private static List<String> mentionNames(String text) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) != '@') {
                continue;
            }
            int j = i + 1;
            while (j < text.length()) {
                char c = text.charAt(j);
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                        || (c >= '0' && c <= '9') || c == '_' || c == '.') {
                    j++;
                } else {
                    break;
                }
            }
            if (j > i + 1) {
                String name = text.substring(i + 1, j).toLowerCase();
                if (!out.contains(name)) {
                    out.add(name);
                }
            }
            i = j - 1;
        }
        return out;
    }

    // ------------------------------------------------------------------
    // accounts
    // ------------------------------------------------------------------

    private Object createAccount(Map<String, Object> args) throws ApiException {
        String displayName = reqString(args, "displayName").trim();
        String username = reqString(args, "username").trim();
        String bio = str(args.get("bio"), "").trim();
        // Login contacts (TikTok-style): stored normalized when given, so
        // loginWithContact can match by phone or email. Kept optional here
        // so accounts created by older app versions keep working.
        String phone = str(args.get("phone"), "").trim().isEmpty()
                ? null : normalizePhone(str(args.get("phone"), ""));
        String email = str(args.get("email"), "").trim().isEmpty()
                ? null : normalizeEmail(str(args.get("email"), ""));
        if (phone != null && !phone.matches("^\\+?\\d{7,15}$")) {
            throw new ApiException("That phone number doesn't look right \u2014 check it and try again.");
        }
        if (email != null && !email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$")) {
            throw new ApiException("That email address doesn't look right \u2014 check it and try again.");
        }
        if (phone != null) {
            for (Map<String, Object> a : accounts) {
                if (phone.equals(str(a.get("phone"), ""))) {
                    throw new ApiException("That phone number is already used by another account \u2014 log in with it instead.");
                }
            }
        }
        if (email != null) {
            for (Map<String, Object> a : accounts) {
                if (email.equals(str(a.get("email"), ""))) {
                    throw new ApiException("That email is already used by another account \u2014 log in with it instead.");
                }
            }
        }
        if (displayName.length() < 1 || displayName.length() > 50) {
            throw new ApiException("Display name must be 1-50 characters.");
        }
        if (username.length() < 3 || username.length() > 20
                || !username.matches("^[a-zA-Z0-9_.]+$")) {
            throw new ApiException("Username can use letters, numbers, dots and underscores only (3-20).");
        }
        if (bio.length() > 150) {
            throw new ApiException("Bio must be 150 characters or less.");
        }
        String lower = username.toLowerCase();
        for (Map<String, Object> a : accounts) {
            if (str(a.get("username"), "").equals(lower)) {
                throw new ApiException("That username is taken \u2014 try another one.");
            }
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextAccountId++);
        row.put("username", lower);
        row.put("displayName", displayName);
        row.put("bio", bio);
        row.put("isPrivate", 0L);
        row.put("isAdmin", 0L);
        row.put("phone", phone);
        row.put("email", email);
        row.put("dateOfBirth", null);
        row.put("region", "");
        row.put("createdAt", nowIso());
        accounts.add(row);
        settingsRow(num(row.get("id"), 0));
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("account", toAccount(row));
        return resp;
    }

    // ------------------------------------------------------------------
    // login \u2014 mirrors the web server fix (v1.3): the identifier may be a
    // phone number, an email address, or a username. The seeded @clippro
    // admin account has no phone or email, so it signs in by username.
    // ------------------------------------------------------------------

    /** Normalizes an email for storage/lookup: trimmed and lowercased. */
    private static String normalizeEmail(String raw) {
        return raw == null ? null : raw.trim().toLowerCase();
    }

    /** Normalizes a phone number: keeps digits and one leading +. */
    private static String normalizePhone(String raw) {
        if (raw == null) {
            return null;
        }
        String cleaned = raw.replaceAll("[\\s()\\-]", "");
        String plus = cleaned.startsWith("+") ? "+" : "";
        return plus + cleaned.replace("+", "").replaceAll("\\D", "");
    }

    /**
     * Matches email, then phone (forgiving a missing or extra leading "+"),
     * then username (case-insensitive, a leading "@" is allowed) \u2014 exactly
     * like the web server's findAccountByIdentifier.
     */
    private Map<String, Object> findAccountByIdentifier(String identifier) {
        String raw = identifier == null ? "" : identifier.trim();
        if (raw.isEmpty()) {
            return null;
        }
        // Only a real email shape takes the email path \u2014 "@clippro" is a
        // username with its @ prefix, not an email address.
        if (raw.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$")) {
            String email = normalizeEmail(raw);
            for (Map<String, Object> a : accounts) {
                if (email.equals(str(a.get("email"), ""))) {
                    return a;
                }
            }
            return null;
        }
        // Only try the phone path when the identifier actually holds a
        // phone number: a digit-less name normalizes to "" and would match
        // the first account with no phone (the @clippro admin) otherwise.
        String phone = normalizePhone(raw);
        if (phone.replace("+", "").length() >= 7) {
            for (Map<String, Object> a : accounts) {
                if (phone.equals(str(a.get("phone"), ""))) {
                    return a;
                }
            }
            String alt = phone.startsWith("+") ? phone.substring(1) : "+" + phone;
            for (Map<String, Object> a : accounts) {
                if (alt.equals(str(a.get("phone"), ""))) {
                    return a;
                }
            }
        }
        String name = raw.toLowerCase().replaceFirst("^@", "");
        for (Map<String, Object> a : accounts) {
            if (name.equals(str(a.get("username"), ""))) {
                return a;
            }
        }
        return null;
    }

    /** Like toAccount, plus the owner's login contacts. */
    private Map<String, Object> toOwnAccount(Map<String, Object> row) {
        Map<String, Object> m = toAccount(row);
        m.put("phone", str(row.get("phone"), null));
        m.put("email", str(row.get("email"), null));
        return m;
    }

    private Object loginWithContact(Map<String, Object> args) throws ApiException {
        String identifier = reqString(args, "identifier").trim();
        if (identifier.length() < 3 || identifier.length() > 120) {
            throw new ApiException("Enter your phone number, email, or username.");
        }
        Map<String, Object> account = findAccountByIdentifier(identifier);
        if (account == null) {
            throw new ApiException("No account found for that phone number, email, or username. Check it, or create a new account below.");
        }
        if (num(account.get("suspended"), 0) == 1) {
            throw new ApiException("This account is suspended. Contact ClipPro support.");
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("account", toOwnAccount(account));
        return resp;
    }

    private Object listAccounts(Map<String, Object> args) {
        List<Map<String, Object>> sorted = new ArrayList<>(accounts);
        sorted.sort((a, b) -> {
            int c = str(b.get("createdAt"), "").compareTo(str(a.get("createdAt"), ""));
            if (c != 0) {
                return c;
            }
            return Long.compare(num(b.get("id"), 0), num(a.get("id"), 0));
        });
        List<Object> out = new ArrayList<>();
        int n = 0;
        for (Map<String, Object> a : sorted) {
            if (n++ >= 100) {
                break;
            }
            Map<String, Object> m = toAccount(a);
            long id = num(a.get("id"), 0);
            m.put("followerCount", countWhere(follows, "followeeId", id));
            m.put("postCount", countWhere(posts, "accountId", id));
            out.add(m);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("accounts", out);
        return resp;
    }

    // ------------------------------------------------------------------
    // feed
    // ------------------------------------------------------------------

    private Object getFeed(Map<String, Object> args) throws ApiException {
        long viewerId = reqLong(args, "viewerId");
        String tab = reqString(args, "tab");
        if (!"foryou".equals(tab) && !"following".equals(tab)) {
            throw new ApiException("Invalid feed tab.");
        }
        long limit = args.containsKey("limit") ? num(args.get("limit"), 5) : 5;
        if (limit < 1) {
            limit = 1;
        }
        if (limit > 10) {
            limit = 10;
        }
        Long cursor = null;
        if (args.get("cursor") instanceof Number) {
            cursor = ((Number) args.get("cursor")).longValue();
        }
        List<Long> followeeIds = new ArrayList<>();
        if ("following".equals(tab)) {
            for (Map<String, Object> f : follows) {
                if (num(f.get("followerId"), -1) == viewerId) {
                    followeeIds.add(num(f.get("followeeId"), 0));
                }
            }
            if (followeeIds.isEmpty()) {
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("posts", new ArrayList<>());
                resp.put("nextCursor", null);
                return resp;
            }
        }
        List<Map<String, Object>> candidates = new ArrayList<>();
        for (Map<String, Object> p : posts) {
            long id = num(p.get("id"), 0);
            if (cursor != null && id >= cursor) {
                continue;
            }
            if ("following".equals(tab)
                    && !followeeIds.contains(num(p.get("accountId"), -1))) {
                continue;
            }
            candidates.add(p);
        }
        candidates.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        if (candidates.size() > limit * 4) {
            candidates = new ArrayList<>(candidates.subList(0, (int) (limit * 4)));
        }
        List<Map<String, Object>> visible = new ArrayList<>();
        for (Map<String, Object> row : candidates) {
            Map<String, Object> author = accountById(num(row.get("accountId"), -1));
            if (author == null) {
                continue;
            }
            if (viewerId > 0 && isHidden(viewerId, num(row.get("id"), 0))) {
                continue;
            }
            if (viewerId > 0 && isMuted(viewerId, num(author.get("id"), 0))) {
                continue;
            }
            if (canViewAuthor(viewerId, author)) {
                visible.add(row);
            }
            if (visible.size() >= limit * 2) {
                break;
            }
        }
        List<Map<String, Object>> ranked = visible;
        if (viewerId > 0) {
            // Content preferences -> Filter keywords: hide matching captions.
            // (Applies to both tabs, like the hosted server.)
            Map<String, Object> vs0 = settingsRow(viewerId);
            List<String> keywords = new ArrayList<>();
            try {
                Object parsed = Json.parse(str(vs0.get("filterKeywords"), "[]"));
                if (parsed instanceof List) {
                    for (Object x : (List<Object>) parsed) {
                        if (x instanceof String) {
                            String k = ((String) x).toLowerCase().trim();
                            if (!k.isEmpty()) {
                                keywords.add(k);
                            }
                        }
                    }
                }
            } catch (Exception ignored) {
            }
            if (!keywords.isEmpty()) {
                List<Map<String, Object>> filtered = new ArrayList<>();
                for (Map<String, Object> p : ranked) {
                    String caption = str(p.get("caption"), "").toLowerCase();
                    boolean hidden = false;
                    for (String k : keywords) {
                        if (caption.contains(k)) {
                            hidden = true;
                            break;
                        }
                    }
                    if (!hidden) {
                        filtered.add(p);
                    }
                }
                ranked = filtered;
            }
        }
        if ("foryou".equals(tab) && viewerId > 0) {
            Map<String, Object> vs = settingsRow(viewerId);
            List<String> interests = new ArrayList<>();
            try {
                Object parsed = Json.parse(str(vs.get("interests"), "[]"));
                if (parsed instanceof List) {
                    for (Object x : (List<Object>) parsed) {
                        if (x instanceof String) {
                            interests.add((String) x);
                        }
                    }
                }
            } catch (Exception ignored) {
            }
            if (!interests.isEmpty()) {
                // Restricted mode: the feed shows only the chosen interest topics.
                if (num(vs.get("restrictedMode"), 0) == 1) {
                    List<Map<String, Object>> only = new ArrayList<>();
                    for (Map<String, Object> p : ranked) {
                        if (interests.contains(str(p.get("category"), ""))) {
                            only.add(p);
                        }
                    }
                    ranked = only;
                }
                // Personalized ads off also turns off interest-based ranking.
                if (num(vs.get("personalizedAds"), 1) == 1) {
                    List<Map<String, Object>> sorted = new ArrayList<>(ranked);
                    sorted.sort((a, b) -> {
                        int am = interests.contains(str(a.get("category"), "")) ? 1 : 0;
                        int bm = interests.contains(str(b.get("category"), "")) ? 1 : 0;
                        if (am != bm) {
                            return bm - am;
                        }
                        return Long.compare(num(b.get("id"), 0), num(a.get("id"), 0));
                    });
                    ranked = sorted;
                }
            }
        }
        List<Map<String, Object>> page = new ArrayList<>();
        for (int i = 0; i < ranked.size() && i < limit; i++) {
            page.add(ranked.get(i));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("posts", hydratePosts(viewerId, page));
        Long nextCursor = null;
        if (candidates.size() >= limit * 4 && !page.isEmpty()) {
            nextCursor = num(page.get(page.size() - 1).get("id"), 0);
        }
        resp.put("nextCursor", nextCursor);
        return resp;
    }

    // ------------------------------------------------------------------
    // posts
    // ------------------------------------------------------------------

    private Object createPost(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String kind = reqString(args, "kind");
        String caption = str(args.get("caption"), "").trim();
        String category = reqString(args, "category");
        String mediaBase64 = reqString(args, "mediaBase64");
        String mimeType = reqString(args, "mimeType");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (!"video".equals(kind) && !"photo".equals(kind)) {
            throw new ApiException("Post must be a video or a photo.");
        }
        boolean okMime = "video".equals(kind) ? mimeType.startsWith("video/")
                : mimeType.startsWith("image/");
        if (!okMime) {
            throw new ApiException("That file type does not match the post type.");
        }
        if (caption.length() > 300) {
            throw new ApiException("Caption must be 300 characters or less.");
        }
        if (!isCategory(category)) {
            throw new ApiException("Unknown category.");
        }
        if (mediaBase64.length() < 1 || mediaBase64.length() > MAX_BASE64_CHARS) {
            throw new ApiException("That file could not be uploaded.");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(mediaBase64);
        } catch (IllegalArgumentException e) {
            throw new ApiException("That file could not be uploaded.");
        }
        if (bytes.length == 0) {
            throw new ApiException("The uploaded file was empty.");
        }
        if (bytes.length > MAX_UPLOAD_BYTES) {
            throw new ApiException("That file is over the 25 MB per-file upload limit.");
        }
        String ext = extForMime(mimeType);
        String key = "posts/" + accountId + "/" + System.currentTimeMillis()
                + "-" + (100_000_000 + random.nextInt(900_000_000)) + "." + ext;
        File f = new File(mediaDir, key);
        if (!f.getParentFile().mkdirs() && !f.getParentFile().isDirectory()) {
            throw new ApiException("Could not save the post.");
        }
        try {
            Files.write(f.toPath(), bytes);
        } catch (IOException e) {
            throw new ApiException("Could not save the post.");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextPostId++);
        row.put("accountId", accountId);
        row.put("kind", kind);
        row.put("mediaBlobKey", key);
        row.put("mimeType", mimeType);
        row.put("caption", caption);
        row.put("category", category);
        row.put("viewCount", 0L);
        row.put("shareCount", 0L);
        row.put("location", str(args.get("location"), ""));
        row.put("commentsOff", commentsOffFor(args) ? 1L : 0L);
        row.put("createdAt", nowIso());
        posts.add(row);
        long postId = num(row.get("id"), 0);
        for (Map<String, Object> fl : follows) {
            if (num(fl.get("followeeId"), -1) == accountId) {
                notify(num(fl.get("followerId"), 0), accountId, "post", postId);
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("id", postId);
        return resp;
    }

    private static String extForMime(String mime) {
        switch (mime) {
            case "video/mp4": return "mp4";
            case "video/webm": return "webm";
            case "video/quicktime": return "mov";
            case "image/png": return "png";
            case "image/webp": return "webp";
            case "image/gif": return "gif";
            default: return "jpg";
        }
    }

    private Object deletePost(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long postId = reqLong(args, "postId");
        Map<String, Object> post = postById(postId);
        if (post == null || num(post.get("accountId"), -1) != accountId) {
            throw new ApiException("You can only delete your own posts.");
        }
        removePostCascade(post);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    /** Deletes a post row, its likes/comments/notifications, and its media file. */
    private void removePostCascade(Map<String, Object> post) {
        long postId = num(post.get("id"), 0);
        notifications.removeIf(n -> num(n.get("postId"), -1) == postId);
        likes.removeIf(l -> num(l.get("postId"), -1) == postId);
        comments.removeIf(c -> num(c.get("postId"), -1) == postId);
        posts.remove(post);
        String key = str(post.get("mediaBlobKey"), "");
        if (!key.isEmpty() && !key.contains("..")) {
            try {
                Files.deleteIfExists(new File(mediaDir, key).toPath());
            } catch (IOException ignored) {
            }
        }
    }

    private Object recordView(Map<String, Object> args) throws ApiException {
        long postId = reqLong(args, "postId");
        Map<String, Object> post = postById(postId);
        if (post != null) {
            post.put("viewCount", num(post.get("viewCount"), 0) + 1);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    /**
     * Records one viewing session of a post (seconds watched, watched to the
     * end). Mirrors the hosted server's recordWatch: only watches of at
     * least 1 second are stored, as watch-event rows that feed the For You
     * ranking and the viewer's taste profile. Does NOT touch viewCount -
     * recordView owns that - so a view is never counted twice.
     */
    private Object recordWatch(Map<String, Object> args) throws ApiException {
        long viewerId = num(args.get("viewerId"), 0);
        long postId = reqLong(args, "postId");
        long watchSeconds = num(args.get("watchSeconds"), 0);
        if (watchSeconds < 0) {
            watchSeconds = 0;
        }
        if (watchSeconds > 3600) {
            watchSeconds = 3600;
        }
        boolean completed = false;
        Object c = args.get("completed");
        if (c instanceof Boolean) {
            completed = (Boolean) c;
        } else if (c instanceof Number) {
            completed = ((Number) c).longValue() != 0;
        }
        if (watchSeconds >= 1) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextWatchId++);
            row.put("postId", postId);
            row.put("accountId", viewerId);
            row.put("watchSeconds", watchSeconds);
            row.put("completed", completed ? 1L : 0L);
            row.put("createdAt", nowIso());
            watches.add(row);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ------------------------------------------------------------------
    // likes / comments / follows
    // ------------------------------------------------------------------

    private Object toggleLike(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long postId = reqLong(args, "postId");
        if (accountById(accountId) == null || postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> l : likes) {
            if (num(l.get("postId"), -1) == postId
                    && num(l.get("accountId"), -1) == accountId) {
                existing = l;
                break;
            }
        }
        boolean liked;
        if (existing != null) {
            likes.remove(existing);
            liked = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextLikeId++);
            row.put("postId", postId);
            row.put("accountId", accountId);
            row.put("createdAt", nowIso());
            likes.add(row);
            liked = true;
            Map<String, Object> post = postById(postId);
            if (post != null) {
                notify(num(post.get("accountId"), 0), accountId, "like", postId);
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("liked", liked);
        resp.put("likeCount", countWhere(likes, "postId", postId));
        return resp;
    }

    private Object listComments(Map<String, Object> args) throws ApiException {
        long postId = reqLong(args, "postId");
        long viewerId = num(args.get("viewerId"), 0);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> c : comments) {
            if (num(c.get("postId"), -1) == postId) {
                rows.add(c);
            }
        }
        rows.sort((a, b) -> {
            int pa = num(a.get("pinned"), 0) == 1 ? 0 : 1;
            int pb = num(b.get("pinned"), 0) == 1 ? 0 : 1;
            if (pa != pb) {
                return pa - pb;
            }
            return Long.compare(num(a.get("id"), 0), num(b.get("id"), 0));
        });
        if (rows.size() > 200) {
            rows = new ArrayList<>(rows.subList(0, 200));
        }
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> c : rows) {
            Map<String, Object> a = accountById(num(c.get("accountId"), -1));
            if (a == null) {
                continue;
            }
            Map<String, Object> author = new LinkedHashMap<>();
            author.put("id", num(a.get("id"), 0));
            author.put("username", str(a.get("username"), ""));
            author.put("displayName", str(a.get("displayName"), ""));
            long cid = num(c.get("id"), 0);
            boolean liked = false;
            if (viewerId > 0) {
                for (Map<String, Object> r : commentLikes) {
                    if (num(r.get("accountId"), -1) == viewerId
                            && num(r.get("commentId"), -1) == cid) {
                        liked = true;
                        break;
                    }
                }
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", cid);
            m.put("text", str(c.get("text"), ""));
            m.put("createdAt", str(c.get("createdAt"), ""));
            m.put("author", author);
            m.put("liked", liked);
            m.put("likeCount", countWhere(commentLikes, "commentId", cid));
            m.put("pinned", num(c.get("pinned"), 0) == 1);
            out.add(m);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("comments", out);
        return resp;
    }

    private Object addComment(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long postId = reqLong(args, "postId");
        String text = reqString(args, "text").trim();
        if (text.length() < 1 || text.length() > 300) {
            throw new ApiException("Comment must be 1-300 characters.");
        }
        Map<String, Object> post = postById(postId);
        if (post == null) {
            throw new ApiException("That post no longer exists.");
        }
        long authorId = num(post.get("accountId"), -1);
        if (num(post.get("commentsOff"), 0) == 1 && authorId != accountId) {
            throw new ApiException("Comments are turned off for this post.");
        }
        if (authorId != accountId) {
            if (isBlockedEitherWay(accountId, authorId)) {
                throw new ApiException("You can't comment on this post.");
            }
            Map<String, Object> s = settingsRow(authorId);
            String perm = str(s.get("commentPermission"), "everyone");
            if ("noone".equals(perm)) {
                throw new ApiException("Comments are turned off for this post.");
            }
            if ("followers".equals(perm) && !isFollowing(accountId, authorId)) {
                throw new ApiException("Only followers can comment on this creator's posts.");
            }
        }
        // Mentions and tags: enforce each mentioned account's mention permission.
        List<Map<String, Object>> mentioned = new ArrayList<>();
        for (String name : mentionNames(text)) {
            Map<String, Object> mAcc = null;
            for (Map<String, Object> a : accounts) {
                if (str(a.get("username"), "").equals(name)) {
                    mAcc = a;
                    break;
                }
            }
            if (mAcc == null || num(mAcc.get("id"), -1) == accountId) {
                continue;
            }
            Map<String, Object> ms = settingsRow(num(mAcc.get("id"), 0));
            String perm = str(ms.get("mentionsPermission"), "everyone");
            if ("noone".equals(perm)) {
                throw new ApiException("You can't mention @" + str(mAcc.get("username"), "") + ".");
            }
            if ("followers".equals(perm)
                    && !isFollowing(accountId, num(mAcc.get("id"), 0))) {
                throw new ApiException("Only followers can mention @"
                        + str(mAcc.get("username"), "") + ".");
            }
            mentioned.add(mAcc);
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextCommentId++);
        row.put("postId", postId);
        row.put("accountId", accountId);
        row.put("text", text);
        row.put("pinned", 0L);
        row.put("createdAt", nowIso());
        comments.add(row);
        notify(authorId, accountId, "comment", postId);
        for (Map<String, Object> mAcc : mentioned) {
            long mId = num(mAcc.get("id"), 0);
            if (mId == authorId) {
                continue;
            }
            Map<String, Object> ms = settingsRow(mId);
            if (num(ms.get("notifyMentions"), 1) == 1) {
                notify(mId, accountId, "comment", postId);
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("id", num(row.get("id"), 0));
        return resp;
    }

    private Object toggleFollow(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long targetId = reqLong(args, "targetId");
        if (accountId == targetId) {
            throw new ApiException("You can't follow yourself.");
        }
        if (accountById(accountId) == null || accountById(targetId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (isBlockedEitherWay(accountId, targetId)) {
            throw new ApiException("You can't follow this account.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> f : follows) {
            if (num(f.get("followerId"), -1) == accountId
                    && num(f.get("followeeId"), -1) == targetId) {
                existing = f;
                break;
            }
        }
        boolean following;
        if (existing != null) {
            follows.remove(existing);
            following = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextFollowId++);
            row.put("followerId", accountId);
            row.put("followeeId", targetId);
            row.put("createdAt", nowIso());
            follows.add(row);
            following = true;
            notify(targetId, accountId, "follow", null);
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("following", following);
        resp.put("followerCount", countWhere(follows, "followeeId", targetId));
        return resp;
    }

    // ------------------------------------------------------------------
    // profile / search / notifications / settings
    // ------------------------------------------------------------------

    private Object getProfile(Map<String, Object> args) throws ApiException {
        long viewerId = reqLong(args, "viewerId");
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        boolean isSelf = viewerId == accountId;
        boolean canView = canViewAuthor(viewerId, account);
        List<Map<String, Object>> postRows = new ArrayList<>();
        List<Map<String, Object>> likedRows = new ArrayList<>();
        if (canView) {
            for (Map<String, Object> p : posts) {
                if (num(p.get("accountId"), -1) == accountId) {
                    postRows.add(p);
                }
            }
            postRows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
            if (postRows.size() > 60) {
                postRows = new ArrayList<>(postRows.subList(0, 60));
            }
            Map<String, Object> ts = settingsRow(accountId);
            boolean showLiked = isSelf || num(ts.get("showLiked"), 1) == 1;
            if (showLiked) {
                List<Long> likedIds = new ArrayList<>();
                for (Map<String, Object> l : likes) {
                    if (num(l.get("accountId"), -1) == accountId) {
                        likedIds.add(num(l.get("postId"), 0));
                    }
                }
                likedIds.sort(Collections.reverseOrder());
                if (likedIds.size() > 60) {
                    likedIds = new ArrayList<>(likedIds.subList(0, 60));
                }
                for (Long pid : likedIds) {
                    Map<String, Object> p = postById(pid);
                    if (p == null) {
                        continue;
                    }
                    Map<String, Object> a = accountById(num(p.get("accountId"), -1));
                    if (a != null && canViewAuthor(viewerId, a)) {
                        likedRows.add(p);
                    }
                }
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("account", toAccount(account));
        resp.put("followerCount", countWhere(follows, "followeeId", accountId));
        resp.put("followingCount", countWhere(follows, "followerId", accountId));
        resp.put("postCount", countWhere(posts, "accountId", accountId));
        resp.put("isFollowing", viewerId > 0 && isFollowing(viewerId, accountId));
        resp.put("isSelf", isSelf);
        boolean viewerBlocked = false;
        if (viewerId > 0 && !isSelf) {
            for (Map<String, Object> bl : blocks) {
                if (num(bl.get("blockerId"), -1) == viewerId
                        && num(bl.get("blockedId"), -1) == accountId) {
                    viewerBlocked = true;
                    break;
                }
            }
        }
        resp.put("viewerBlocked", viewerBlocked);
        resp.put("canView", canView);
        List<Map<String, Object>> repostedRows = new ArrayList<>();
        if (canView) {
            List<Long> repostedIds = new ArrayList<>();
            for (Map<String, Object> r : reposts) {
                if (num(r.get("accountId"), -1) == accountId) {
                    repostedIds.add(num(r.get("postId"), 0));
                }
            }
            repostedIds.sort(Collections.reverseOrder());
            if (repostedIds.size() > 60) {
                repostedIds = new ArrayList<>(repostedIds.subList(0, 60));
            }
            for (Long pid : repostedIds) {
                Map<String, Object> p = postById(pid);
                if (p == null) {
                    continue;
                }
                Map<String, Object> a = accountById(num(p.get("accountId"), -1));
                if (a != null && canViewAuthor(viewerId, a)) {
                    repostedRows.add(p);
                }
            }
        }
        resp.put("posts", hydratePosts(viewerId, postRows));
        resp.put("likedPosts", hydratePosts(viewerId, likedRows));
        resp.put("repostedPosts", hydratePosts(viewerId, repostedRows));
        return resp;
    }

    private Object search(Map<String, Object> args) throws ApiException {
        long viewerId = reqLong(args, "viewerId");
        String query = str(args.get("query"), "").trim();
        String category = args.get("category") instanceof String
                ? (String) args.get("category") : null;
        String q = query.toLowerCase();
        List<Map<String, Object>> allAccounts = new ArrayList<>(accounts);
        allAccounts.sort((a, b) -> {
            int c = str(b.get("createdAt"), "").compareTo(str(a.get("createdAt"), ""));
            if (c != 0) {
                return c;
            }
            return Long.compare(num(b.get("id"), 0), num(a.get("id"), 0));
        });
        if (allAccounts.size() > 200) {
            allAccounts = new ArrayList<>(allAccounts.subList(0, 200));
        }
        List<Object> matchedAccounts = new ArrayList<>();
        for (Map<String, Object> a : allAccounts) {
            if (matchedAccounts.size() >= 20) {
                break;
            }
            if (!query.isEmpty()
                    && !str(a.get("username"), "").toLowerCase().contains(q)
                    && !str(a.get("displayName"), "").toLowerCase().contains(q)) {
                continue;
            }
            // Empty search = Discover: only accounts that opted into suggestions.
            if (query.isEmpty()
                    && num(settingsRow(num(a.get("id"), 0)).get("suggestAccount"), 1) == 0) {
                continue;
            }
            Map<String, Object> m = toAccount(a);
            m.put("followerCount", countWhere(follows, "followeeId", num(a.get("id"), 0)));
            matchedAccounts.add(m);
        }
        List<Map<String, Object>> postRows = new ArrayList<>(posts);
        postRows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        if (postRows.size() > 120) {
            postRows = new ArrayList<>(postRows.subList(0, 120));
        }
        List<Map<String, Object>> visible = new ArrayList<>();
        for (Map<String, Object> p : postRows) {
            if (category != null && !category.equals(str(p.get("category"), ""))) {
                continue;
            }
            if (!query.isEmpty()) {
                Map<String, Object> a = accountById(num(p.get("accountId"), -1));
                String authorName = a == null ? ""
                        : (str(a.get("username"), "") + " " + str(a.get("displayName"), ""))
                                .toLowerCase();
                if (!str(p.get("caption"), "").toLowerCase().contains(q)
                        && !authorName.contains(q)) {
                    continue;
                }
            }
            Map<String, Object> a = accountById(num(p.get("accountId"), -1));
            if (a != null && canViewAuthor(viewerId, a)) {
                visible.add(p);
            }
            if (visible.size() >= 30) {
                break;
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("accounts", matchedAccounts);
        resp.put("posts", hydratePosts(viewerId, visible));
        return resp;
    }

    private Object getNotifications(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> n : notifications) {
            if (num(n.get("accountId"), -1) == accountId) {
                rows.add(n);
            }
        }
        rows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        if (rows.size() > 50) {
            rows = new ArrayList<>(rows.subList(0, 50));
        }
        long unread = 0;
        for (Map<String, Object> n : rows) {
            if (num(n.get("read"), 0) == 0) {
                unread++;
            }
        }
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> n : rows) {
            Map<String, Object> actor = accountById(num(n.get("actorId"), -1));
            if (actor == null) {
                continue;
            }
            Map<String, Object> actorJson = new LinkedHashMap<>();
            actorJson.put("id", num(actor.get("id"), 0));
            actorJson.put("username", str(actor.get("username"), ""));
            actorJson.put("displayName", str(actor.get("displayName"), ""));
            String caption = null;
            Object pid = n.get("postId");
            if (pid instanceof Number) {
                Map<String, Object> p = postById(((Number) pid).longValue());
                if (p != null) {
                    caption = str(p.get("caption"), "");
                }
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", num(n.get("id"), 0));
            m.put("type", str(n.get("type"), ""));
            m.put("read", num(n.get("read"), 0) == 1);
            m.put("createdAt", str(n.get("createdAt"), ""));
            m.put("postId", pid instanceof Number ? ((Number) pid).longValue() : null);
            m.put("postCaption", caption);
            m.put("body", n.get("body") instanceof String ? (String) n.get("body") : null);
            m.put("actor", actorJson);
            out.add(m);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("unread", unread);
        resp.put("notifications", out);
        return resp;
    }

    private Object markNotificationsRead(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        for (Map<String, Object> n : notifications) {
            if (num(n.get("accountId"), -1) == accountId) {
                n.put("read", 1L);
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object getSettings(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("settings", toSettings(account));
        return resp;
    }

    private Object updateSettings(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> s = settingsRow(accountId);
        if (args.containsKey("isPrivate")) {
            boolean v = bool(args.get("isPrivate"), false);
            account.put("isPrivate", v ? 1L : 0L);
        }
        putBool(s, args, "autoplay");
        putBool(s, args, "dataSaver");
        putBool(s, args, "notifyLikes");
        putBool(s, args, "notifyComments");
        putBool(s, args, "notifyFollowers");
        putBool(s, args, "notifyNewPosts");
        putBool(s, args, "showLiked");
        putBool(s, args, "allowDownloads");
        putBool(s, args, "suggestAccount");
        putBool(s, args, "restrictedMode");
        putBool(s, args, "personalizedAds");
        putBool(s, args, "pushEnabled");
        putBool(s, args, "reduceMotion");
        putBool(s, args, "notifyMentions");
        putBool(s, args, "securityAlerts");
        putPermission(s, args, "mentionsPermission");
        putPermission(s, args, "dmPermission");
        putPermission(s, args, "duetPermission");
        putPermission(s, args, "stitchPermission");
        if (args.get("textSize") instanceof String) {
            String v = (String) args.get("textSize");
            if (!"small".equals(v) && !"normal".equals(v) && !"large".equals(v)) {
                throw new ApiException("Invalid text size.");
            }
            s.put("textSize", v);
        }
        if (args.get("screenTimeLimit") instanceof Number) {
            long v = ((Number) args.get("screenTimeLimit")).longValue();
            if (v < 0 || v > 480) {
                throw new ApiException("Invalid screen time limit.");
            }
            s.put("screenTimeLimit", v);
        }
        if (args.get("filterKeywords") instanceof List) {
            List<Object> list = (List<Object>) args.get("filterKeywords");
            if (list.size() > 30) {
                throw new ApiException("Too many filter keywords.");
            }
            List<String> clean = new ArrayList<>();
            for (Object x : list) {
                if (!(x instanceof String)) {
                    throw new ApiException("Invalid filter keyword.");
                }
                String k = ((String) x).trim();
                if (k.length() < 1 || k.length() > 40) {
                    throw new ApiException("Invalid filter keyword.");
                }
                clean.add(k);
            }
            s.put("filterKeywords", Json.stringify(new ArrayList<Object>(clean)));
        }
        if (args.get("videoQuality") instanceof String) {
            String v = (String) args.get("videoQuality");
            if (!"auto".equals(v) && !"high".equals(v) && !"saver".equals(v)) {
                throw new ApiException("Invalid video quality.");
            }
            s.put("videoQuality", v);
        }
        if (args.get("commentPermission") instanceof String) {
            String v = (String) args.get("commentPermission");
            if (!"everyone".equals(v) && !"followers".equals(v) && !"noone".equals(v)) {
                throw new ApiException("Invalid comment permission.");
            }
            s.put("commentPermission", v);
        }
        if (args.get("viewPermission") instanceof String) {
            String v = (String) args.get("viewPermission");
            if (!"everyone".equals(v) && !"followers".equals(v) && !"onlyme".equals(v)) {
                throw new ApiException("Invalid view permission.");
            }
            s.put("viewPermission", v);
        }
        if (args.get("language") instanceof String) {
            String v = (String) args.get("language");
            if (!"en".equals(v) && !"bn".equals(v)) {
                throw new ApiException("Invalid language.");
            }
            s.put("language", v);
        }
        if (args.get("interests") instanceof List) {
            List<Object> list = (List<Object>) args.get("interests");
            if (list.size() > 12) {
                throw new ApiException("Too many interests.");
            }
            List<String> clean = new ArrayList<>();
            for (Object x : list) {
                if (!(x instanceof String) || !isCategory((String) x)) {
                    throw new ApiException("Invalid interest.");
                }
                clean.add((String) x);
            }
            List<Object> stored = new ArrayList<>(clean);
            s.put("interests", Json.stringify(stored));
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("settings", toSettings(account));
        return resp;
    }

    private static void putBool(Map<String, Object> s, Map<String, Object> args, String key) {
        if (args.containsKey(key)) {
            s.put(key, bool(args.get(key), false) ? 1L : 0L);
        }
    }

    private static void putPermission(Map<String, Object> s, Map<String, Object> args,
                                      String key) throws ApiException {
        if (args.get(key) instanceof String) {
            String v = (String) args.get(key);
            if (!"everyone".equals(v) && !"followers".equals(v) && !"noone".equals(v)) {
                throw new ApiException("Invalid " + key + ".");
            }
            s.put(key, v);
        }
    }

    private Object updateProfile(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String displayName = reqString(args, "displayName").trim();
        String bio = reqString(args, "bio").trim();
        if (displayName.length() < 1 || displayName.length() > 50) {
            throw new ApiException("Display name must be 1-50 characters.");
        }
        if (bio.length() > 150) {
            throw new ApiException("Bio must be 150 characters or less.");
        }
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        account.put("displayName", displayName);
        account.put("bio", bio);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("account", toAccount(account));
        return resp;
    }

    // ------------------------------------------------------------------
    // admin / account management / support (v1.1)
    // ------------------------------------------------------------------

    /** Throws unless the account exists and carries the admin flag. */
    private Map<String, Object> requireAdmin(long accountId) throws ApiException {
        Map<String, Object> account = accountById(accountId);
        if (account == null || num(account.get("isAdmin"), 0) != 1) {
            throw new ApiException("Admin access required.");
        }
        return account;
    }

    /** Deletes an account and everything attached to it. */
    private void removeAccountCascade(long accountId) {
        List<Map<String, Object>> ownPosts = new ArrayList<>();
        for (Map<String, Object> p : posts) {
            if (num(p.get("accountId"), -1) == accountId) {
                ownPosts.add(p);
            }
        }
        for (Map<String, Object> p : ownPosts) {
            removePostCascade(p);
        }
        likes.removeIf(l -> num(l.get("accountId"), -1) == accountId);
        comments.removeIf(c -> num(c.get("accountId"), -1) == accountId);
        follows.removeIf(f -> num(f.get("followerId"), -1) == accountId
                || num(f.get("followeeId"), -1) == accountId);
        notifications.removeIf(n -> num(n.get("accountId"), -1) == accountId
                || num(n.get("actorId"), -1) == accountId);
        blocks.removeIf(b -> num(b.get("blockerId"), -1) == accountId
                || num(b.get("blockedId"), -1) == accountId);
        List<Map<String, Object>> ownThreads = new ArrayList<>();
        for (Map<String, Object> t : supportThreads) {
            if (num(t.get("accountId"), -1) == accountId) {
                ownThreads.add(t);
            }
        }
        for (Map<String, Object> t : ownThreads) {
            long tid = num(t.get("id"), 0);
            supportMessages.removeIf(m -> num(m.get("threadId"), -1) == tid);
        }
        supportThreads.removeIf(t -> num(t.get("accountId"), -1) == accountId);
        supportMessages.removeIf(m -> num(m.get("senderId"), -1) == accountId);
        settings.remove(String.valueOf(accountId));
        accounts.removeIf(a -> num(a.get("id"), -1) == accountId);
    }

    private Object updateAccountDetails(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String username = reqString(args, "username").trim();
        if (username.length() < 3 || username.length() > 20
                || !username.matches("^[a-zA-Z0-9_.]+$")) {
            throw new ApiException("Username can use letters, numbers, dots and underscores only (3-20).");
        }
        String lower = username.toLowerCase();
        for (Map<String, Object> a : accounts) {
            if (str(a.get("username"), "").equals(lower)
                    && num(a.get("id"), -1) != accountId) {
                throw new ApiException("That username is taken \u2014 try another one.");
            }
        }
        String dob = null;
        if (args.get("dateOfBirth") instanceof String) {
            String d = ((String) args.get("dateOfBirth")).trim();
            if (!d.isEmpty()) {
                if (!d.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
                    throw new ApiException("Date of birth must look like 2000-01-31.");
                }
                dob = d;
            }
        }
        String region = str(args.get("region"), "").trim();
        if (region.length() > 56) {
            throw new ApiException("Region must be 56 characters or less.");
        }
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        account.put("username", lower);
        account.put("dateOfBirth", dob);
        account.put("region", region);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("account", toAccount(account));
        return resp;
    }

    private Object toggleBlock(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long targetId = reqLong(args, "targetId");
        if (accountId == targetId) {
            throw new ApiException("You can't block yourself.");
        }
        if (accountById(accountId) == null || accountById(targetId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> b : blocks) {
            if (num(b.get("blockerId"), -1) == accountId
                    && num(b.get("blockedId"), -1) == targetId) {
                existing = b;
                break;
            }
        }
        boolean blocked;
        if (existing != null) {
            blocks.remove(existing);
            blocked = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextBlockId++);
            row.put("blockerId", accountId);
            row.put("blockedId", targetId);
            row.put("createdAt", nowIso());
            blocks.add(row);
            follows.removeIf(f -> (num(f.get("followerId"), -1) == accountId
                            && num(f.get("followeeId"), -1) == targetId)
                    || (num(f.get("followerId"), -1) == targetId
                            && num(f.get("followeeId"), -1) == accountId));
            blocked = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("blocked", blocked);
        return resp;
    }

    private Object listBlocked(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> b : blocks) {
            if (num(b.get("blockerId"), -1) == accountId) {
                rows.add(b);
            }
        }
        rows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> b : rows) {
            Map<String, Object> a = accountById(num(b.get("blockedId"), -1));
            if (a != null) {
                out.add(toAccount(a));
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("accounts", out);
        return resp;
    }

    private Object exportMyData(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        List<Map<String, Object>> postRows = new ArrayList<>();
        for (Map<String, Object> p : posts) {
            if (num(p.get("accountId"), -1) == accountId) {
                postRows.add(p);
            }
        }
        postRows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        if (postRows.size() > 500) {
            postRows = new ArrayList<>(postRows.subList(0, 500));
        }
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> p : postRows) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", num(p.get("id"), 0));
            m.put("kind", str(p.get("kind"), ""));
            m.put("caption", str(p.get("caption"), ""));
            m.put("category", str(p.get("category"), ""));
            m.put("viewCount", num(p.get("viewCount"), 0));
            m.put("createdAt", str(p.get("createdAt"), ""));
            out.add(m);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("exportedAt", nowIso());
        resp.put("account", toAccount(account));
        resp.put("settings", toSettings(account));
        resp.put("followerCount", countWhere(follows, "followeeId", accountId));
        resp.put("followingCount", countWhere(follows, "followerId", accountId));
        resp.put("posts", out);
        return resp;
    }

    private Object deleteMyAccount(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        removeAccountCascade(accountId);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object adminOverview(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        requireAdmin(adminId);
        long openThreads = 0;
        for (Map<String, Object> t : supportThreads) {
            if ("open".equals(str(t.get("status"), ""))) {
                openThreads++;
            }
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("accounts", (long) accounts.size());
        stats.put("posts", (long) posts.size());
        stats.put("comments", (long) comments.size());
        stats.put("likes", (long) likes.size());
        stats.put("openThreads", openThreads);
        List<Map<String, Object>> sorted = new ArrayList<>(accounts);
        sorted.sort((a, b) -> {
            int c = str(b.get("createdAt"), "").compareTo(str(a.get("createdAt"), ""));
            if (c != 0) {
                return c;
            }
            return Long.compare(num(b.get("id"), 0), num(a.get("id"), 0));
        });
        if (sorted.size() > 200) {
            sorted = new ArrayList<>(sorted.subList(0, 200));
        }
        List<Object> accOut = new ArrayList<>();
        for (Map<String, Object> a : sorted) {
            Map<String, Object> m = toAccount(a);
            long id = num(a.get("id"), 0);
            m.put("followerCount", countWhere(follows, "followeeId", id));
            m.put("postCount", countWhere(posts, "accountId", id));
            accOut.add(m);
        }
        List<Map<String, Object>> postRows = new ArrayList<>(posts);
        postRows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        if (postRows.size() > 50) {
            postRows = new ArrayList<>(postRows.subList(0, 50));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("stats", stats);
        resp.put("accounts", accOut);
        resp.put("posts", hydratePosts(adminId, postRows));
        return resp;
    }

    private Object adminDeletePost(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        long postId = reqLong(args, "postId");
        requireAdmin(adminId);
        Map<String, Object> post = postById(postId);
        if (post == null) {
            throw new ApiException("That post no longer exists.");
        }
        long authorId = num(post.get("accountId"), -1);
        removePostCascade(post);
        if (authorId != adminId) {
            notify(authorId, adminId, "system", null);
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object adminRemoveAccount(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        long targetId = reqLong(args, "targetId");
        requireAdmin(adminId);
        if (targetId == adminId) {
            throw new ApiException("You can't remove your own admin account here.");
        }
        Map<String, Object> target = accountById(targetId);
        if (target == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (num(target.get("isAdmin"), 0) == 1) {
            throw new ApiException("Admin accounts can't be removed.");
        }
        removeAccountCascade(targetId);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ---- support threads ----

    private Map<String, Object> threadOwner(long accountId) {
        Map<String, Object> owner = accountById(accountId);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", accountId);
        m.put("username", owner == null ? "" : str(owner.get("username"), ""));
        m.put("displayName", owner == null ? "" : str(owner.get("displayName"), ""));
        return m;
    }

    private Map<String, Object> threadSummary(Map<String, Object> thread) {
        long threadId = num(thread.get("id"), 0);
        long count = 0;
        Map<String, Object> last = null;
        for (Map<String, Object> m : supportMessages) {
            if (num(m.get("threadId"), -1) == threadId) {
                count++;
                if (last == null || num(m.get("id"), 0) > num(last.get("id"), 0)) {
                    last = m;
                }
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", threadId);
        out.put("topic", str(thread.get("topic"), ""));
        out.put("subject", str(thread.get("subject"), ""));
        out.put("status", str(thread.get("status"), ""));
        out.put("createdAt", str(thread.get("createdAt"), ""));
        out.put("updatedAt", str(thread.get("updatedAt"), ""));
        out.put("messageCount", count);
        out.put("lastMessage", last == null ? null : str(last.get("body"), ""));
        out.put("lastFromAdmin", last != null && num(last.get("fromAdmin"), 0) == 1);
        out.put("owner", threadOwner(num(thread.get("accountId"), 0)));
        return out;
    }

    private static final String[] SUPPORT_TOPICS = {
            "Account", "Posting", "Comments", "Safety", "Technical", "Other"
    };

    private Object createSupportThread(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String topic = reqString(args, "topic");
        String subject = reqString(args, "subject").trim();
        String message = reqString(args, "message").trim();
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        boolean okTopic = false;
        for (String t : SUPPORT_TOPICS) {
            if (t.equals(topic)) {
                okTopic = true;
                break;
            }
        }
        if (!okTopic) {
            throw new ApiException("Unknown support topic.");
        }
        if (subject.length() < 1 || subject.length() > 120) {
            throw new ApiException("Subject must be 1-120 characters.");
        }
        if (message.length() < 1 || message.length() > 2000) {
            throw new ApiException("Message must be 1-2000 characters.");
        }
        Map<String, Object> thread = new LinkedHashMap<>();
        thread.put("id", nextThreadId++);
        thread.put("accountId", accountId);
        thread.put("topic", topic);
        thread.put("subject", subject);
        thread.put("status", "open");
        thread.put("createdAt", nowIso());
        thread.put("updatedAt", str(thread.get("createdAt"), ""));
        supportThreads.add(thread);
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("id", nextMessageId++);
        msg.put("threadId", num(thread.get("id"), 0));
        msg.put("senderId", accountId);
        msg.put("fromAdmin", 0L);
        msg.put("body", message);
        msg.put("createdAt", nowIso());
        supportMessages.add(msg);
        for (Map<String, Object> a : accounts) {
            if (num(a.get("isAdmin"), 0) == 1) {
                notify(num(a.get("id"), 0), accountId, "support", null);
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("threadId", num(thread.get("id"), 0));
        return resp;
    }

    private List<Map<String, Object>> sortedThreads() {
        List<Map<String, Object>> rows = new ArrayList<>(supportThreads);
        rows.sort((a, b) -> {
            int c = str(b.get("updatedAt"), "").compareTo(str(a.get("updatedAt"), ""));
            if (c != 0) {
                return c;
            }
            return Long.compare(num(b.get("id"), 0), num(a.get("id"), 0));
        });
        return rows;
    }

    private Object listMySupportThreads(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        List<Object> out = new ArrayList<>();
        int n = 0;
        for (Map<String, Object> t : sortedThreads()) {
            if (num(t.get("accountId"), -1) != accountId) {
                continue;
            }
            if (n++ >= 50) {
                break;
            }
            out.add(threadSummary(t));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("threads", out);
        return resp;
    }

    private Object adminListSupportThreads(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        requireAdmin(adminId);
        List<Object> out = new ArrayList<>();
        int n = 0;
        for (Map<String, Object> t : sortedThreads()) {
            if (n++ >= 100) {
                break;
            }
            out.add(threadSummary(t));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("threads", out);
        return resp;
    }

    private Map<String, Object> threadById(long threadId) {
        for (Map<String, Object> t : supportThreads) {
            if (num(t.get("id"), -1) == threadId) {
                return t;
            }
        }
        return null;
    }

    private Object getSupportThread(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long threadId = reqLong(args, "threadId");
        Map<String, Object> thread = threadById(threadId);
        if (thread == null) {
            throw new ApiException("That support thread no longer exists.");
        }
        Map<String, Object> caller = accountById(accountId);
        boolean isOwner = num(thread.get("accountId"), -1) == accountId;
        if (caller == null
                || (!isOwner && num(caller.get("isAdmin"), 0) != 1)) {
            throw new ApiException("You can't open this support thread.");
        }
        List<Map<String, Object>> msgs = new ArrayList<>();
        for (Map<String, Object> m : supportMessages) {
            if (num(m.get("threadId"), -1) == threadId) {
                msgs.add(m);
            }
        }
        msgs.sort((a, b) -> Long.compare(num(a.get("id"), 0), num(b.get("id"), 0)));
        if (msgs.size() > 300) {
            msgs = new ArrayList<>(msgs.subList(0, 300));
        }
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> m : msgs) {
            Map<String, Object> sender = accountById(num(m.get("senderId"), -1));
            if (sender == null) {
                continue;
            }
            Map<String, Object> senderJson = new LinkedHashMap<>();
            senderJson.put("id", num(sender.get("id"), 0));
            senderJson.put("username", str(sender.get("username"), ""));
            senderJson.put("displayName", str(sender.get("displayName"), ""));
            Map<String, Object> mm = new LinkedHashMap<>();
            mm.put("id", num(m.get("id"), 0));
            mm.put("body", str(m.get("body"), ""));
            mm.put("fromAdmin", num(m.get("fromAdmin"), 0) == 1);
            mm.put("createdAt", str(m.get("createdAt"), ""));
            mm.put("sender", senderJson);
            out.add(mm);
        }
        Map<String, Object> threadJson = new LinkedHashMap<>();
        threadJson.put("id", num(thread.get("id"), 0));
        threadJson.put("topic", str(thread.get("topic"), ""));
        threadJson.put("subject", str(thread.get("subject"), ""));
        threadJson.put("status", str(thread.get("status"), ""));
        threadJson.put("createdAt", str(thread.get("createdAt"), ""));
        threadJson.put("updatedAt", str(thread.get("updatedAt"), ""));
        threadJson.put("owner", threadOwner(num(thread.get("accountId"), 0)));
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("thread", threadJson);
        resp.put("messages", out);
        return resp;
    }

    private Object replySupportThread(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long threadId = reqLong(args, "threadId");
        String body = reqString(args, "body").trim();
        if (body.length() < 1 || body.length() > 2000) {
            throw new ApiException("Reply must be 1-2000 characters.");
        }
        Map<String, Object> thread = threadById(threadId);
        if (thread == null) {
            throw new ApiException("That support thread no longer exists.");
        }
        Map<String, Object> caller = accountById(accountId);
        boolean isOwner = num(thread.get("accountId"), -1) == accountId;
        if (caller == null
                || (!isOwner && num(caller.get("isAdmin"), 0) != 1)) {
            throw new ApiException("You can't reply to this support thread.");
        }
        boolean fromAdmin = num(caller.get("isAdmin"), 0) == 1 && !isOwner;
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("id", nextMessageId++);
        msg.put("threadId", threadId);
        msg.put("senderId", accountId);
        msg.put("fromAdmin", fromAdmin ? 1L : 0L);
        msg.put("body", body);
        msg.put("createdAt", nowIso());
        supportMessages.add(msg);
        thread.put("status", fromAdmin ? "answered" : "open");
        thread.put("updatedAt", nowIso());
        if (fromAdmin) {
            notify(num(thread.get("accountId"), 0), accountId, "support", null);
        } else {
            for (Map<String, Object> a : accounts) {
                if (num(a.get("isAdmin"), 0) == 1) {
                    notify(num(a.get("id"), 0), accountId, "support", null);
                }
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("id", num(msg.get("id"), 0));
        return resp;
    }

    private Object closeSupportThread(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long threadId = reqLong(args, "threadId");
        Map<String, Object> thread = threadById(threadId);
        if (thread == null) {
            throw new ApiException("That support thread no longer exists.");
        }
        Map<String, Object> caller = accountById(accountId);
        if (caller == null
                || (num(thread.get("accountId"), -1) != accountId
                        && num(caller.get("isAdmin"), 0) != 1)) {
            throw new ApiException("You can't close this support thread.");
        }
        thread.put("status", "closed");
        thread.put("updatedAt", nowIso());
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ------------------------------------------------------------------
    // starter content
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // v1.5: saves, reposts, hides, mutes, comment likes/pins, reports,
    // stories, messages, groups, collections, wallet, payouts, LIVE,
    // admin broadcast/audit, auth security. Every action the shipped
    // web client can call is implemented here so the phone app never
    // shows "Unknown action" for a button the user can tap.
    // ------------------------------------------------------------------

    private long nextRowId(List<Map<String, Object>> rows) {
        return maxId(rows) + 1;
    }

    private boolean isHidden(long viewerId, long postId) {
        for (Map<String, Object> r : hiddenPosts) {
            if (num(r.get("accountId"), -1) == viewerId
                    && num(r.get("postId"), -1) == postId) {
                return true;
            }
        }
        return false;
    }

    private boolean isMuted(long viewerId, long targetId) {
        for (Map<String, Object> r : mutes) {
            if (num(r.get("accountId"), -1) == viewerId
                    && num(r.get("targetId"), -1) == targetId) {
                return true;
            }
        }
        return false;
    }

    private boolean isSaved(long viewerId, long postId) {
        for (Map<String, Object> r : saves) {
            if (num(r.get("accountId"), -1) == viewerId
                    && num(r.get("postId"), -1) == postId) {
                return true;
            }
        }
        return false;
    }

    private void logAdmin(long adminId, String action, String targetType,
            Long targetId, String detail) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(auditLog));
        row.put("adminId", adminId);
        row.put("action", action);
        row.put("targetType", targetType);
        row.put("targetId", targetId);
        row.put("detail", detail == null ? "" : detail);
        row.put("createdAt", nowIso());
        auditLog.add(row);
    }

    private Map<String, Object> securityOf(Map<String, Object> account) {
        Map<String, Object> s = new LinkedHashMap<>();
        Object pw = account.get("password");
        s.put("hasPassword", pw instanceof String && !((String) pw).isEmpty());
        s.put("email", str(account.get("email"), null));
        s.put("phone", str(account.get("phone"), null));
        s.put("emailVerified", num(account.get("emailVerified"), 0) == 1);
        s.put("phoneVerified", num(account.get("phoneVerified"), 0) == 1);
        s.put("twoFactorEnabled", num(account.get("twoFactorEnabled"), 0) == 1);
        return s;
    }

    private long coinsOf(Map<String, Object> account) {
        return num(account.get("coins"), 0);
    }

    private long diamondsOf(Map<String, Object> account) {
        return num(account.get("diamonds"), 0);
    }

    /** Official notice: lands in the inbox regardless of toggles. */
    private void notifyBody(long recipientId, long actorId, String type,
            Long postId, String body) {
        if (recipientId == actorId) {
            return;
        }
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("id", nextNotifId++);
        n.put("accountId", recipientId);
        n.put("actorId", actorId);
        n.put("type", type);
        n.put("postId", postId);
        n.put("body", body);
        n.put("read", 0L);
        n.put("createdAt", nowIso());
        notifications.add(n);
    }

    private String saveMedia(String mediaBase64, String mimeType, String dir,
            long accountId) throws ApiException {
        if (mediaBase64 == null || mediaBase64.length() < 1
                || mediaBase64.length() > MAX_BASE64_CHARS) {
            throw new ApiException("That file could not be uploaded.");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(mediaBase64);
        } catch (IllegalArgumentException e) {
            throw new ApiException("That file could not be uploaded.");
        }
        if (bytes.length == 0) {
            throw new ApiException("The uploaded file was empty.");
        }
        if (bytes.length > MAX_UPLOAD_BYTES) {
            throw new ApiException("That file is over the 25 MB per-file upload limit.");
        }
        String ext = extForMime(mimeType);
        String key = dir + "/" + accountId + "/" + System.currentTimeMillis()
                + "-" + (100_000_000 + random.nextInt(900_000_000)) + "." + ext;
        File f = new File(mediaDir, key);
        if (!f.getParentFile().mkdirs() && !f.getParentFile().isDirectory()) {
            throw new ApiException("Could not save the file.");
        }
        try {
            Files.write(f.toPath(), bytes);
        } catch (IOException e) {
            throw new ApiException("Could not save the file.");
        }
        return key;
    }

    private boolean commentsOffFor(Map<String, Object> args) {
        if (args.get("commentsOff") instanceof Boolean) {
            return (Boolean) args.get("commentsOff");
        }
        if (args.get("allowComments") instanceof Boolean) {
            return !(Boolean) args.get("allowComments");
        }
        return false;
    }

    private Map<String, Object> personJson(Map<String, Object> account) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", num(account.get("id"), 0));
        m.put("username", str(account.get("username"), ""));
        m.put("displayName", str(account.get("displayName"), ""));
        return m;
    }

    private static Map<String, Object> giftJson(String id, String name,
            String icon, long coins) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", name);
        m.put("icon", icon);
        m.put("coins", coins);
        return m;
    }

    private static List<Map<String, Object>> giftCatalog() {
        List<Map<String, Object>> out = new ArrayList<>();
        out.add(giftJson("rose", "Rose", "\uD83C\uDF39", 1));
        out.add(giftJson("fingerheart", "Finger Heart", "\uD83E\uDEF0", 5));
        out.add(giftJson("heart", "Heart", "\u2764\uFE0F", 10));
        out.add(giftJson("star", "Star", "\u2B50", 25));
        out.add(giftJson("crown", "Crown", "\uD83D\uDC51", 99));
        out.add(giftJson("lion", "Lion", "\uD83E\uDD81", 299));
        out.add(giftJson("rocket", "Rocket", "\uD83D\uDE80", 500));
        out.add(giftJson("universe", "Universe", "\uD83C\uDF0C", 1000));
        return out;
    }

    private static long packCoins(String packId) {
        switch (packId) {
            case "p70": return 70;
            case "p350": return 350;
            case "p700": return 700;
            case "p1400": return 1400;
            case "p3500": return 3500;
            default: return -1;
        }
    }

    private static long packCents(String packId) {
        switch (packId) {
            case "p70": return 99;
            case "p350": return 499;
            case "p700": return 999;
            case "p1400": return 1999;
            case "p3500": return 4999;
            default: return -1;
        }
    }

    private static String packPrice(String packId) {
        switch (packId) {
            case "p70": return "$0.99";
            case "p350": return "$4.99";
            case "p700": return "$9.99";
            case "p1400": return "$19.99";
            case "p3500": return "$49.99";
            default: return "";
        }
    }

    private static List<Map<String, Object>> packCatalog() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String id : new String[]{"p70", "p350", "p700", "p1400", "p3500"}) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("coins", packCoins(id));
            m.put("priceLabel", packPrice(id));
            out.add(m);
        }
        return out;
    }

    private Map<String, Object> giftTxJson(Map<String, Object> tx) {
        Map<String, Object> sender = accountById(num(tx.get("senderId"), -1));
        Map<String, Object> receiver = accountById(num(tx.get("receiverId"), -1));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", num(tx.get("id"), 0));
        m.put("giftId", str(tx.get("giftId"), ""));
        m.put("giftName", str(tx.get("giftName"), ""));
        m.put("coins", num(tx.get("coins"), 0));
        m.put("diamonds", num(tx.get("diamonds"), 0));
        Object pid = tx.get("postId");
        m.put("postId", pid instanceof Number ? ((Number) pid).longValue() : null);
        Object sid = tx.get("liveSessionId");
        m.put("liveSessionId", sid instanceof Number ? ((Number) sid).longValue() : null);
        m.put("createdAt", str(tx.get("createdAt"), ""));
        m.put("sender", sender == null ? personJson(new LinkedHashMap<>()) : personJson(sender));
        m.put("receiver", receiver == null ? personJson(new LinkedHashMap<>()) : personJson(receiver));
        return m;
    }

    private String issueCode(long accountId, String identifier, String purpose) {
        String code = String.valueOf(1000 + random.nextInt(9000));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(verificationCodes));
        row.put("accountId", accountId);
        row.put("identifier", identifier == null ? "" : identifier);
        row.put("purpose", purpose);
        row.put("code", code);
        row.put("expiresAtMs", System.currentTimeMillis() + 10 * 60 * 1000);
        row.put("used", 0L);
        row.put("createdAt", nowIso());
        verificationCodes.add(row);
        return code;
    }

    private boolean consumeCode(long accountId, String purpose, String code) {
        Map<String, Object> best = null;
        for (Map<String, Object> r : verificationCodes) {
            if (num(r.get("accountId"), -1) != accountId) {
                continue;
            }
            if (!purpose.equals(str(r.get("purpose"), ""))) {
                continue;
            }
            if (num(r.get("used"), 0) != 0) {
                continue;
            }
            if (best == null || num(r.get("id"), 0) > num(best.get("id"), 0)) {
                best = r;
            }
        }
        if (best == null) {
            return false;
        }
        if (num(best.get("expiresAtMs"), 0) < System.currentTimeMillis()) {
            return false;
        }
        if (!str(best.get("code"), "").equals(code == null ? "" : code.trim())) {
            return false;
        }
        best.put("used", 1L);
        return true;
    }

    private boolean consumeCodeByIdentifier(String identifier, String purpose,
            String code) {
        Map<String, Object> best = null;
        for (Map<String, Object> r : verificationCodes) {
            if (!str(r.get("identifier"), "").equals(identifier)) {
                continue;
            }
            if (!purpose.equals(str(r.get("purpose"), ""))) {
                continue;
            }
            if (num(r.get("used"), 0) != 0) {
                continue;
            }
            if (best == null || num(r.get("id"), 0) > num(best.get("id"), 0)) {
                best = r;
            }
        }
        if (best == null) {
            return false;
        }
        if (num(best.get("expiresAtMs"), 0) < System.currentTimeMillis()) {
            return false;
        }
        if (!str(best.get("code"), "").equals(code == null ? "" : code.trim())) {
            return false;
        }
        best.put("used", 1L);
        return true;
    }

    // ------------------------------------------------------------------
    // saves / reposts / hide / mute / share
    // ------------------------------------------------------------------

    private Object toggleSave(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long postId = reqLong(args, "postId");
        if (accountById(accountId) == null || postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> r : saves) {
            if (num(r.get("accountId"), -1) == accountId
                    && num(r.get("postId"), -1) == postId) {
                existing = r;
                break;
            }
        }
        boolean saved;
        if (existing != null) {
            saves.remove(existing);
            saved = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(saves));
            row.put("accountId", accountId);
            row.put("postId", postId);
            row.put("createdAt", nowIso());
            saves.add(row);
            saved = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("saved", saved);
        return resp;
    }

    private Object toggleRepost(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long postId = reqLong(args, "postId");
        if (accountById(accountId) == null || postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> r : reposts) {
            if (num(r.get("accountId"), -1) == accountId
                    && num(r.get("postId"), -1) == postId) {
                existing = r;
                break;
            }
        }
        boolean reposted;
        if (existing != null) {
            reposts.remove(existing);
            reposted = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(reposts));
            row.put("accountId", accountId);
            row.put("postId", postId);
            row.put("createdAt", nowIso());
            reposts.add(row);
            reposted = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("reposted", reposted);
        return resp;
    }

    private Object hidePost(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long postId = reqLong(args, "postId");
        if (accountById(accountId) == null || postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        if (!isHidden(accountId, postId)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(hiddenPosts));
            row.put("accountId", accountId);
            row.put("postId", postId);
            row.put("createdAt", nowIso());
            hiddenPosts.add(row);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object toggleMute(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long targetId = reqLong(args, "targetId");
        if (accountById(accountId) == null || accountById(targetId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> r : mutes) {
            if (num(r.get("accountId"), -1) == accountId
                    && num(r.get("targetId"), -1) == targetId) {
                existing = r;
                break;
            }
        }
        boolean muted;
        if (existing != null) {
            mutes.remove(existing);
            muted = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(mutes));
            row.put("accountId", accountId);
            row.put("targetId", targetId);
            row.put("createdAt", nowIso());
            mutes.add(row);
            muted = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("muted", muted);
        return resp;
    }

    private Object sharePost(Map<String, Object> args) throws ApiException {
        long postId = reqLong(args, "postId");
        Map<String, Object> post = postById(postId);
        if (post == null) {
            throw new ApiException("That post no longer exists.");
        }
        post.put("shareCount", num(post.get("shareCount"), 0) + 1);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("shareCount", num(post.get("shareCount"), 0));
        return resp;
    }

    private Object listSavedPosts(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long viewerId = num(args.get("viewerId"), accountId);
        List<Long> ids = new ArrayList<>();
        for (Map<String, Object> r : saves) {
            if (num(r.get("accountId"), -1) == accountId) {
                ids.add(num(r.get("postId"), 0));
            }
        }
        ids.sort(Collections.reverseOrder());
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Long pid : ids) {
            Map<String, Object> p = postById(pid);
            if (p == null) {
                continue;
            }
            Map<String, Object> a = accountById(num(p.get("accountId"), -1));
            if (a != null && canViewAuthor(viewerId, a)) {
                rows.add(p);
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("posts", hydratePosts(viewerId, rows));
        return resp;
    }

    // ------------------------------------------------------------------
    // comments: likes, pins, deletes
    // ------------------------------------------------------------------

    private Map<String, Object> commentById(long id) {
        for (Map<String, Object> c : comments) {
            if (num(c.get("id"), -1) == id) {
                return c;
            }
        }
        return null;
    }

    private Object toggleCommentLike(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long commentId = reqLong(args, "commentId");
        if (accountById(accountId) == null || commentById(commentId) == null) {
            throw new ApiException("That comment no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> r : commentLikes) {
            if (num(r.get("accountId"), -1) == accountId
                    && num(r.get("commentId"), -1) == commentId) {
                existing = r;
                break;
            }
        }
        boolean liked;
        if (existing != null) {
            commentLikes.remove(existing);
            liked = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(commentLikes));
            row.put("accountId", accountId);
            row.put("commentId", commentId);
            row.put("createdAt", nowIso());
            commentLikes.add(row);
            liked = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("liked", liked);
        resp.put("likeCount", countWhere(commentLikes, "commentId", commentId));
        return resp;
    }

    private Object togglePinComment(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long commentId = reqLong(args, "commentId");
        Map<String, Object> comment = commentById(commentId);
        if (comment == null) {
            throw new ApiException("That comment no longer exists.");
        }
        Map<String, Object> post = postById(num(comment.get("postId"), -1));
        if (post == null) {
            throw new ApiException("That post no longer exists.");
        }
        boolean isAuthor = num(post.get("accountId"), -1) == accountId;
        Map<String, Object> me = accountById(accountId);
        boolean isAdmin = me != null && num(me.get("isAdmin"), 0) == 1;
        if (!isAuthor && !isAdmin) {
            throw new ApiException("Only the creator can pin a comment.");
        }
        boolean pinned = num(comment.get("pinned"), 0) == 0;
        comment.put("pinned", pinned ? 1L : 0L);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("pinned", pinned);
        return resp;
    }

    private Object deleteComment(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long commentId = reqLong(args, "commentId");
        Map<String, Object> comment = commentById(commentId);
        if (comment == null) {
            throw new ApiException("That comment no longer exists.");
        }
        Map<String, Object> post = postById(num(comment.get("postId"), -1));
        boolean isOwner = num(comment.get("accountId"), -1) == accountId;
        boolean isAuthor = post != null && num(post.get("accountId"), -1) == accountId;
        Map<String, Object> me = accountById(accountId);
        boolean isAdmin = me != null && num(me.get("isAdmin"), 0) == 1;
        if (!isOwner && !isAuthor && !isAdmin) {
            throw new ApiException("You can't delete this comment.");
        }
        comments.remove(comment);
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> r : commentLikes) {
            if (num(r.get("commentId"), -1) == commentId) {
                dead.add(r);
            }
        }
        commentLikes.removeAll(dead);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ------------------------------------------------------------------
    // stories
    // ------------------------------------------------------------------

    private boolean storyActive(Map<String, Object> story) {
        try {
            long created = Instant.parse(str(story.get("createdAt"), "")).toEpochMilli();
            return System.currentTimeMillis() - created < 24L * 60 * 60 * 1000;
        } catch (Exception e) {
            return true;
        }
    }

    private Object createStory(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String kind = reqString(args, "kind");
        String caption = str(args.get("caption"), "").trim();
        String mediaBase64 = reqString(args, "mediaBase64");
        String mimeType = reqString(args, "mimeType");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (!"video".equals(kind) && !"photo".equals(kind)) {
            throw new ApiException("Story must be a video or a photo.");
        }
        boolean okMime = "video".equals(kind) ? mimeType.startsWith("video/")
                : mimeType.startsWith("image/");
        if (!okMime) {
            throw new ApiException("That file type does not match the story type.");
        }
        if (caption.length() > 150) {
            throw new ApiException("Caption must be 150 characters or less.");
        }
        String key = saveMedia(mediaBase64, mimeType, "stories", accountId);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(stories));
        row.put("accountId", accountId);
        row.put("kind", kind);
        row.put("caption", caption);
        row.put("mediaBlobKey", key);
        row.put("mimeType", mimeType);
        row.put("createdAt", nowIso());
        stories.add(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("id", num(row.get("id"), 0));
        return resp;
    }

    private Object deleteStory(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long storyId = reqLong(args, "storyId");
        Map<String, Object> story = null;
        for (Map<String, Object> s : stories) {
            if (num(s.get("id"), -1) == storyId) {
                story = s;
                break;
            }
        }
        if (story == null) {
            throw new ApiException("That story no longer exists.");
        }
        if (num(story.get("accountId"), -1) != accountId) {
            throw new ApiException("You can only delete your own story.");
        }
        stories.remove(story);
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> r : storyLikes) {
            if (num(r.get("storyId"), -1) == storyId) {
                dead.add(r);
            }
        }
        storyLikes.removeAll(dead);
        List<Map<String, Object>> deadViews = new ArrayList<>();
        for (Map<String, Object> r : storyViews) {
            if (num(r.get("storyId"), -1) == storyId) {
                deadViews.add(r);
            }
        }
        storyViews.removeAll(deadViews);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object toggleStoryLike(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long storyId = reqLong(args, "storyId");
        Map<String, Object> story = null;
        for (Map<String, Object> s : stories) {
            if (num(s.get("id"), -1) == storyId) {
                story = s;
                break;
            }
        }
        if (story == null || !storyActive(story)) {
            throw new ApiException("That story no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> r : storyLikes) {
            if (num(r.get("accountId"), -1) == accountId
                    && num(r.get("storyId"), -1) == storyId) {
                existing = r;
                break;
            }
        }
        boolean liked;
        if (existing != null) {
            storyLikes.remove(existing);
            liked = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(storyLikes));
            row.put("accountId", accountId);
            row.put("storyId", storyId);
            row.put("createdAt", nowIso());
            storyLikes.add(row);
            liked = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("liked", liked);
        return resp;
    }

    private Object listStories(Map<String, Object> args) {
        long viewerId = num(args.get("viewerId"), 0);
        // Group active stories by author, newest story first.
        List<Map<String, Object>> active = new ArrayList<>();
        for (Map<String, Object> s : stories) {
            if (storyActive(s)) {
                active.add(s);
            }
        }
        active.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        Map<Long, List<Map<String, Object>>> byAuthor = new LinkedHashMap<>();
        for (Map<String, Object> s : active) {
            long authorId = num(s.get("accountId"), -1);
            Map<String, Object> author = accountById(authorId);
            if (author == null || !canViewAuthor(viewerId, author)) {
                continue;
            }
            List<Map<String, Object>> list = byAuthor.get(authorId);
            if (list == null) {
                list = new ArrayList<>();
                byAuthor.put(authorId, list);
            }
            list.add(s);
        }
        boolean changed = false;
        List<Object> groups = new ArrayList<>();
        for (Map.Entry<Long, List<Map<String, Object>>> e : byAuthor.entrySet()) {
            Map<String, Object> author = accountById(e.getKey());
            List<Object> storyJson = new ArrayList<>();
            for (Map<String, Object> s : e.getValue()) {
                long sid = num(s.get("id"), 0);
                if (viewerId > 0) {
                    boolean seen = false;
                    for (Map<String, Object> v : storyViews) {
                        if (num(v.get("accountId"), -1) == viewerId
                                && num(v.get("storyId"), -1) == sid) {
                            seen = true;
                            break;
                        }
                    }
                    if (!seen) {
                        Map<String, Object> v = new LinkedHashMap<>();
                        v.put("id", nextRowId(storyViews));
                        v.put("accountId", viewerId);
                        v.put("storyId", sid);
                        v.put("createdAt", nowIso());
                        storyViews.add(v);
                        changed = true;
                    }
                }
                boolean liked = false;
                if (viewerId > 0) {
                    for (Map<String, Object> r : storyLikes) {
                        if (num(r.get("accountId"), -1) == viewerId
                                && num(r.get("storyId"), -1) == sid) {
                            liked = true;
                            break;
                        }
                    }
                }
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("id", sid);
                m.put("kind", str(s.get("kind"), ""));
                m.put("mediaUrl", "/media/" + str(s.get("mediaBlobKey"), ""));
                m.put("caption", str(s.get("caption"), ""));
                m.put("createdAt", str(s.get("createdAt"), ""));
                m.put("liked", liked);
                m.put("likeCount", countWhere(storyLikes, "storyId", sid));
                m.put("viewCount", countWhere(storyViews, "storyId", sid));
                storyJson.add(m);
            }
            Map<String, Object> g = new LinkedHashMap<>();
            g.put("author", personJson(author));
            g.put("stories", storyJson);
            groups.add(g);
        }
        if (changed) {
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("groups", groups);
        return resp;
    }

    // ------------------------------------------------------------------
    // direct messages and groups
    // ------------------------------------------------------------------

    private Map<String, Object> conversationBetween(long a, long b) {
        long lo = Math.min(a, b);
        long hi = Math.max(a, b);
        for (Map<String, Object> c : conversations) {
            if (num(c.get("aId"), -1) == lo && num(c.get("bId"), -1) == hi) {
                return c;
            }
        }
        return null;
    }

    private Map<String, Object> messageJson(Map<String, Object> msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", num(msg.get("id"), 0));
        m.put("senderId", num(msg.get("senderId"), 0));
        m.put("body", str(msg.get("body"), ""));
        Object pid = msg.get("postId");
        m.put("postId", pid instanceof Number ? ((Number) pid).longValue() : null);
        String key = str(msg.get("mediaBlobKey"), "");
        m.put("mediaUrl", key.isEmpty() ? null : "/media/" + key);
        m.put("mediaMime", str(msg.get("mediaMime"), null));
        m.put("createdAt", str(msg.get("createdAt"), ""));
        return m;
    }

    private Object openConversation(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long targetId = reqLong(args, "targetId");
        if (accountById(accountId) == null || accountById(targetId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (accountId == targetId) {
            throw new ApiException("You can't message yourself.");
        }
        if (isBlockedEitherWay(accountId, targetId)) {
            throw new ApiException("You can't message this account.");
        }
        Map<String, Object> target = accountById(targetId);
        Map<String, Object> ts = settingsRow(targetId);
        String perm = str(ts.get("dmPermission"), "everyone");
        if ("noone".equals(perm)) {
            throw new ApiException("This account doesn't accept messages.");
        }
        if ("followers".equals(perm) && !isFollowing(accountId, targetId)) {
            throw new ApiException("Only followers can message this account.");
        }
        Map<String, Object> conv = conversationBetween(accountId, targetId);
        if (conv == null) {
            conv = new LinkedHashMap<>();
            conv.put("id", nextRowId(conversations));
            conv.put("aId", Math.min(accountId, targetId));
            conv.put("bId", Math.max(accountId, targetId));
            conv.put("createdAt", nowIso());
            conv.put("updatedAt", nowIso());
            conversations.add(conv);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("conversationId", num(conv.get("id"), 0));
        return resp;
    }

    private Object listConversations(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        List<Map<String, Object>> mine = new ArrayList<>();
        for (Map<String, Object> c : conversations) {
            if (num(c.get("aId"), -1) == accountId || num(c.get("bId"), -1) == accountId) {
                mine.add(c);
            }
        }
        mine.sort((a, b) -> str(b.get("updatedAt"), "").compareTo(str(a.get("updatedAt"), "")));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> c : mine) {
            long otherId = num(c.get("aId"), -1) == accountId
                    ? num(c.get("bId"), 0) : num(c.get("aId"), 0);
            Map<String, Object> other = accountById(otherId);
            if (other == null) {
                continue;
            }
            long cid = num(c.get("id"), 0);
            Map<String, Object> last = null;
            for (Map<String, Object> m : messages) {
                if (num(m.get("conversationId"), -1) == cid) {
                    if (last == null || num(m.get("id"), 0) > num(last.get("id"), 0)) {
                        last = m;
                    }
                }
            }
            long unread = 0;
            for (Map<String, Object> m : messages) {
                if (num(m.get("conversationId"), -1) == cid
                        && num(m.get("senderId"), -1) != accountId
                        && num(m.get("read"), 0) == 0) {
                    unread++;
                }
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", cid);
            item.put("updatedAt", str(c.get("updatedAt"), ""));
            item.put("other", personJson(other));
            String lastBody = null;
            String lastAt = null;
            if (last != null) {
                lastBody = str(last.get("body"), "");
                if (lastBody.isEmpty() && last.get("postId") instanceof Number) {
                    lastBody = "Shared a post";
                }
                if (lastBody.isEmpty() && !str(last.get("mediaBlobKey"), "").isEmpty()) {
                    lastBody = "Sent a photo";
                }
                lastAt = str(last.get("createdAt"), "");
            }
            item.put("lastMessage", lastBody);
            item.put("lastAt", lastAt);
            item.put("unread", unread);
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("conversations", out);
        return resp;
    }

    private Object listMessages(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long conversationId = reqLong(args, "conversationId");
        Map<String, Object> conv = null;
        for (Map<String, Object> c : conversations) {
            if (num(c.get("id"), -1) == conversationId) {
                conv = c;
                break;
            }
        }
        if (conv == null) {
            throw new ApiException("That conversation no longer exists.");
        }
        if (num(conv.get("aId"), -1) != accountId
                && num(conv.get("bId"), -1) != accountId) {
            throw new ApiException("That conversation isn't yours.");
        }
        long otherId = num(conv.get("aId"), -1) == accountId
                ? num(conv.get("bId"), 0) : num(conv.get("aId"), 0);
        Map<String, Object> other = accountById(otherId);
        if (other == null) {
            throw new ApiException("That account no longer exists.");
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> m : messages) {
            if (num(m.get("conversationId"), -1) == conversationId) {
                rows.add(m);
            }
        }
        rows.sort((a, b) -> Long.compare(num(a.get("id"), 0), num(b.get("id"), 0)));
        boolean changed = false;
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> m : rows) {
            if (num(m.get("senderId"), -1) != accountId && num(m.get("read"), 0) == 0) {
                m.put("read", 1L);
                changed = true;
            }
            out.add(messageJson(m));
        }
        if (changed) {
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("messages", out);
        resp.put("other", personJson(other));
        return resp;
    }

    private Object sendMessage(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long conversationId = reqLong(args, "conversationId");
        String body = str(args.get("body"), "").trim();
        Long postId = args.get("postId") instanceof Number
                ? ((Number) args.get("postId")).longValue() : null;
        if (body.isEmpty() && postId == null) {
            throw new ApiException("Write something first.");
        }
        if (body.length() > 1000) {
            throw new ApiException("Message must be 1000 characters or less.");
        }
        Map<String, Object> conv = null;
        for (Map<String, Object> c : conversations) {
            if (num(c.get("id"), -1) == conversationId) {
                conv = c;
                break;
            }
        }
        if (conv == null) {
            throw new ApiException("That conversation no longer exists.");
        }
        if (num(conv.get("aId"), -1) != accountId
                && num(conv.get("bId"), -1) != accountId) {
            throw new ApiException("That conversation isn't yours.");
        }
        if (postId != null && postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(messages));
        row.put("conversationId", conversationId);
        row.put("senderId", accountId);
        row.put("body", body);
        row.put("postId", postId);
        row.put("mediaBlobKey", "");
        row.put("mediaMime", null);
        row.put("read", 0L);
        row.put("createdAt", nowIso());
        messages.add(row);
        conv.put("updatedAt", nowIso());
        long otherId = num(conv.get("aId"), -1) == accountId
                ? num(conv.get("bId"), 0) : num(conv.get("aId"), 0);
        notify(otherId, accountId, "message", postId);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object sendMediaMessage(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String scope = str(args.get("scope"), "dm");
        String mediaBase64 = reqString(args, "mediaBase64");
        String mimeType = reqString(args, "mimeType");
        if (!mimeType.startsWith("image/") && !mimeType.startsWith("video/")) {
            throw new ApiException("That file type can't be sent as a message.");
        }
        String key = saveMedia(mediaBase64, mimeType, "messages", accountId);
        if ("group".equals(scope)) {
            long groupId = reqLong(args, "groupId");
            Map<String, Object> group = null;
            for (Map<String, Object> g : groups) {
                if (num(g.get("id"), -1) == groupId) {
                    group = g;
                    break;
                }
            }
            if (group == null) {
                throw new ApiException("That group no longer exists.");
            }
            boolean member = false;
            for (Map<String, Object> m : groupMembers) {
                if (num(m.get("groupId"), -1) == groupId
                        && num(m.get("accountId"), -1) == accountId) {
                    member = true;
                    break;
                }
            }
            if (!member) {
                throw new ApiException("You're not in that group.");
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(groupMessages));
            row.put("groupId", groupId);
            row.put("senderId", accountId);
            row.put("body", "");
            row.put("postId", null);
            row.put("mediaBlobKey", key);
            row.put("mediaMime", mimeType);
            row.put("createdAt", nowIso());
            groupMessages.add(row);
            group.put("updatedAt", nowIso());
            saveAll();
        } else {
            long conversationId = reqLong(args, "conversationId");
            Map<String, Object> conv = null;
            for (Map<String, Object> c : conversations) {
                if (num(c.get("id"), -1) == conversationId) {
                    conv = c;
                    break;
                }
            }
            if (conv == null) {
                throw new ApiException("That conversation no longer exists.");
            }
            if (num(conv.get("aId"), -1) != accountId
                    && num(conv.get("bId"), -1) != accountId) {
                throw new ApiException("That conversation isn't yours.");
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(messages));
            row.put("conversationId", conversationId);
            row.put("senderId", accountId);
            row.put("body", "");
            row.put("postId", null);
            row.put("mediaBlobKey", key);
            row.put("mediaMime", mimeType);
            row.put("read", 0L);
            row.put("createdAt", nowIso());
            messages.add(row);
            conv.put("updatedAt", nowIso());
            long otherId = num(conv.get("aId"), -1) == accountId
                    ? num(conv.get("bId"), 0) : num(conv.get("aId"), 0);
            notify(otherId, accountId, "message", null);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private boolean isGroupMember(long groupId, long accountId) {
        for (Map<String, Object> m : groupMembers) {
            if (num(m.get("groupId"), -1) == groupId
                    && num(m.get("accountId"), -1) == accountId) {
                return true;
            }
        }
        return false;
    }

    private Object createGroup(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String name = reqString(args, "name").trim();
        Object memberIdsObj = args.get("memberIds");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (name.length() < 1 || name.length() > 60) {
            throw new ApiException("Group name must be 1-60 characters.");
        }
        List<Long> memberIds = new ArrayList<>();
        if (memberIdsObj instanceof List) {
            for (Object o : (List<Object>) memberIdsObj) {
                if (o instanceof Number) {
                    memberIds.add(((Number) o).longValue());
                }
            }
        }
        if (memberIds.isEmpty() || memberIds.size() > 50) {
            throw new ApiException("Add at least one member to the group.");
        }
        for (Long mid : memberIds) {
            if (accountById(mid) == null) {
                throw new ApiException("One of those accounts no longer exists.");
            }
        }
        Map<String, Object> group = new LinkedHashMap<>();
        group.put("id", nextRowId(groups));
        group.put("accountId", accountId);
        group.put("name", name);
        group.put("createdAt", nowIso());
        group.put("updatedAt", nowIso());
        groups.add(group);
        long gid = num(group.get("id"), 0);
        Map<String, Object> creator = new LinkedHashMap<>();
        creator.put("id", nextRowId(groupMembers));
        creator.put("groupId", gid);
        creator.put("accountId", accountId);
        creator.put("createdAt", nowIso());
        groupMembers.add(creator);
        for (Long mid : memberIds) {
            if (mid == accountId) {
                continue;
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", nextRowId(groupMembers));
            m.put("groupId", gid);
            m.put("accountId", mid);
            m.put("createdAt", nowIso());
            groupMembers.add(m);
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("groupId", gid);
        return resp;
    }

    private Object addGroupMember(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long groupId = reqLong(args, "groupId");
        long targetId = reqLong(args, "targetId");
        Map<String, Object> group = null;
        for (Map<String, Object> g : groups) {
            if (num(g.get("id"), -1) == groupId) {
                group = g;
                break;
            }
        }
        if (group == null) {
            throw new ApiException("That group no longer exists.");
        }
        if (!isGroupMember(groupId, accountId)) {
            throw new ApiException("You're not in that group.");
        }
        if (accountById(targetId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (!isGroupMember(groupId, targetId)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", nextRowId(groupMembers));
            m.put("groupId", groupId);
            m.put("accountId", targetId);
            m.put("createdAt", nowIso());
            groupMembers.add(m);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object listGroups(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        List<Map<String, Object>> mine = new ArrayList<>();
        for (Map<String, Object> g : groups) {
            if (isGroupMember(num(g.get("id"), 0), accountId)) {
                mine.add(g);
            }
        }
        mine.sort((a, b) -> str(b.get("updatedAt"), "").compareTo(str(a.get("updatedAt"), "")));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> g : mine) {
            long gid = num(g.get("id"), 0);
            long memberCount = 0;
            for (Map<String, Object> m : groupMembers) {
                if (num(m.get("groupId"), -1) == gid) {
                    memberCount++;
                }
            }
            Map<String, Object> last = null;
            for (Map<String, Object> m : groupMessages) {
                if (num(m.get("groupId"), -1) == gid) {
                    if (last == null || num(m.get("id"), 0) > num(last.get("id"), 0)) {
                        last = m;
                    }
                }
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", gid);
            item.put("name", str(g.get("name"), ""));
            item.put("memberCount", memberCount);
            String lastBody = null;
            String lastAt = null;
            if (last != null) {
                lastBody = str(last.get("body"), "");
                if (lastBody.isEmpty() && last.get("postId") instanceof Number) {
                    lastBody = "Shared a post";
                }
                if (lastBody.isEmpty() && !str(last.get("mediaBlobKey"), "").isEmpty()) {
                    lastBody = "Sent a photo";
                }
                lastAt = str(last.get("createdAt"), "");
            }
            item.put("lastMessage", lastBody);
            item.put("lastAt", lastAt);
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("groups", out);
        return resp;
    }

    private Object listGroupMessages(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long groupId = reqLong(args, "groupId");
        Map<String, Object> group = null;
        for (Map<String, Object> g : groups) {
            if (num(g.get("id"), -1) == groupId) {
                group = g;
                break;
            }
        }
        if (group == null) {
            throw new ApiException("That group no longer exists.");
        }
        if (!isGroupMember(groupId, accountId)) {
            throw new ApiException("You're not in that group.");
        }
        List<Object> members = new ArrayList<>();
        for (Map<String, Object> m : groupMembers) {
            if (num(m.get("groupId"), -1) == groupId) {
                Map<String, Object> a = accountById(num(m.get("accountId"), -1));
                if (a != null) {
                    members.add(personJson(a));
                }
            }
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> m : groupMessages) {
            if (num(m.get("groupId"), -1) == groupId) {
                rows.add(m);
            }
        }
        rows.sort((a, b) -> Long.compare(num(a.get("id"), 0), num(b.get("id"), 0)));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> m : rows) {
            Map<String, Object> sender = accountById(num(m.get("senderId"), -1));
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", num(m.get("id"), 0));
            item.put("body", str(m.get("body"), ""));
            Object pid = m.get("postId");
            item.put("postId", pid instanceof Number ? ((Number) pid).longValue() : null);
            String key = str(m.get("mediaBlobKey"), "");
            item.put("mediaUrl", key.isEmpty() ? null : "/media/" + key);
            item.put("mediaMime", str(m.get("mediaMime"), null));
            item.put("createdAt", str(m.get("createdAt"), ""));
            item.put("sender", sender == null ? personJson(new LinkedHashMap<>()) : personJson(sender));
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("name", str(group.get("name"), ""));
        resp.put("members", members);
        resp.put("messages", out);
        return resp;
    }

    private Object sendGroupMessage(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long groupId = reqLong(args, "groupId");
        String body = str(args.get("body"), "").trim();
        Long postId = args.get("postId") instanceof Number
                ? ((Number) args.get("postId")).longValue() : null;
        String mediaBase64 = args.get("mediaBase64") instanceof String
                ? (String) args.get("mediaBase64") : "";
        String mimeType = str(args.get("mimeType"), "");
        if (body.isEmpty() && postId == null && mediaBase64.isEmpty()) {
            throw new ApiException("Write something first.");
        }
        if (body.length() > 1000) {
            throw new ApiException("Message must be 1000 characters or less.");
        }
        Map<String, Object> group = null;
        for (Map<String, Object> g : groups) {
            if (num(g.get("id"), -1) == groupId) {
                group = g;
                break;
            }
        }
        if (group == null) {
            throw new ApiException("That group no longer exists.");
        }
        if (!isGroupMember(groupId, accountId)) {
            throw new ApiException("You're not in that group.");
        }
        if (postId != null && postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        String key = "";
        if (!mediaBase64.isEmpty()) {
            if (!mimeType.startsWith("image/") && !mimeType.startsWith("video/")) {
                throw new ApiException("That file type can't be sent as a message.");
            }
            key = saveMedia(mediaBase64, mimeType, "messages", accountId);
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(groupMessages));
        row.put("groupId", groupId);
        row.put("senderId", accountId);
        row.put("body", body);
        row.put("postId", postId);
        row.put("mediaBlobKey", key);
        row.put("mediaMime", key.isEmpty() ? null : mimeType);
        row.put("createdAt", nowIso());
        groupMessages.add(row);
        group.put("updatedAt", nowIso());
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ------------------------------------------------------------------
    // collections and editor projects
    // ------------------------------------------------------------------

    private Object createCollection(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String name = reqString(args, "name").trim();
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (name.length() < 1 || name.length() > 40) {
            throw new ApiException("Collection name must be 1-40 characters.");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(collections));
        row.put("accountId", accountId);
        row.put("name", name);
        row.put("createdAt", nowIso());
        collections.add(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("id", num(row.get("id"), 0));
        return resp;
    }

    private Object deleteCollection(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long collectionId = reqLong(args, "collectionId");
        Map<String, Object> col = null;
        for (Map<String, Object> c : collections) {
            if (num(c.get("id"), -1) == collectionId) {
                col = c;
                break;
            }
        }
        if (col == null) {
            throw new ApiException("That collection no longer exists.");
        }
        if (num(col.get("accountId"), -1) != accountId) {
            throw new ApiException("That collection isn't yours.");
        }
        collections.remove(col);
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> r : collectionPosts) {
            if (num(r.get("collectionId"), -1) == collectionId) {
                dead.add(r);
            }
        }
        collectionPosts.removeAll(dead);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object toggleCollectionPost(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long collectionId = reqLong(args, "collectionId");
        long postId = reqLong(args, "postId");
        Map<String, Object> col = null;
        for (Map<String, Object> c : collections) {
            if (num(c.get("id"), -1) == collectionId) {
                col = c;
                break;
            }
        }
        if (col == null) {
            throw new ApiException("That collection no longer exists.");
        }
        if (num(col.get("accountId"), -1) != accountId) {
            throw new ApiException("That collection isn't yours.");
        }
        if (postById(postId) == null) {
            throw new ApiException("That post no longer exists.");
        }
        Map<String, Object> existing = null;
        for (Map<String, Object> r : collectionPosts) {
            if (num(r.get("collectionId"), -1) == collectionId
                    && num(r.get("postId"), -1) == postId) {
                existing = r;
                break;
            }
        }
        boolean added;
        if (existing != null) {
            collectionPosts.remove(existing);
            added = false;
        } else {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(collectionPosts));
            row.put("collectionId", collectionId);
            row.put("postId", postId);
            row.put("createdAt", nowIso());
            collectionPosts.add(row);
            added = true;
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("added", added);
        return resp;
    }

    private Object listCollections(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long viewerId = num(args.get("viewerId"), accountId);
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> c : collections) {
            if (num(c.get("accountId"), -1) != accountId) {
                continue;
            }
            long cid = num(c.get("id"), 0);
            List<Long> ids = new ArrayList<>();
            for (Map<String, Object> r : collectionPosts) {
                if (num(r.get("collectionId"), -1) == cid) {
                    ids.add(num(r.get("postId"), 0));
                }
            }
            ids.sort(Collections.reverseOrder());
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Long pid : ids) {
                Map<String, Object> p = postById(pid);
                if (p == null) {
                    continue;
                }
                Map<String, Object> a = accountById(num(p.get("accountId"), -1));
                if (a != null && canViewAuthor(viewerId, a)) {
                    rows.add(p);
                }
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", cid);
            item.put("name", str(c.get("name"), ""));
            item.put("posts", hydratePosts(viewerId, rows));
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("collections", out);
        return resp;
    }

    private Object saveEditorProject(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String title = reqString(args, "title").trim();
        String dataJson = reqString(args, "dataJson");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (title.length() < 1 || title.length() > 80) {
            throw new ApiException("Project title must be 1-80 characters.");
        }
        if (dataJson.length() > 200_000) {
            throw new ApiException("That project is too large to save.");
        }
        Map<String, Object> row = null;
        if (args.get("projectId") instanceof Number) {
            long pid = ((Number) args.get("projectId")).longValue();
            for (Map<String, Object> p : editorProjects) {
                if (num(p.get("id"), -1) == pid) {
                    row = p;
                    break;
                }
            }
            if (row != null && num(row.get("accountId"), -1) != accountId) {
                throw new ApiException("That project isn't yours.");
            }
        }
        if (row == null) {
            row = new LinkedHashMap<>();
            row.put("id", nextRowId(editorProjects));
            row.put("accountId", accountId);
            editorProjects.add(row);
        }
        row.put("title", title);
        row.put("dataJson", dataJson);
        row.put("updatedAt", nowIso());
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("projectId", num(row.get("id"), 0));
        return resp;
    }

    private Object listEditorProjects(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> p : editorProjects) {
            if (num(p.get("accountId"), -1) == accountId) {
                rows.add(p);
            }
        }
        rows.sort((a, b) -> str(b.get("updatedAt"), "").compareTo(str(a.get("updatedAt"), "")));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> p : rows) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", num(p.get("id"), 0));
            item.put("title", str(p.get("title"), ""));
            item.put("dataJson", str(p.get("dataJson"), ""));
            item.put("updatedAt", str(p.get("updatedAt"), ""));
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("projects", out);
        return resp;
    }

    private Object deleteEditorProject(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long projectId = reqLong(args, "projectId");
        Map<String, Object> row = null;
        for (Map<String, Object> p : editorProjects) {
            if (num(p.get("id"), -1) == projectId) {
                row = p;
                break;
            }
        }
        if (row == null) {
            throw new ApiException("That project no longer exists.");
        }
        if (num(row.get("accountId"), -1) != accountId) {
            throw new ApiException("That project isn't yours.");
        }
        editorProjects.remove(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ------------------------------------------------------------------
    // wallet, gifts, payments, payouts
    // ------------------------------------------------------------------

    private Object getWallet(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("coins", coinsOf(account));
        resp.put("diamonds", diamondsOf(account));
        resp.put("diamondsCents", Math.round(diamondsOf(account) * 0.5));
        resp.put("gifts", giftCatalog());
        resp.put("packs", packCatalog());
        return resp;
    }

    private Object buyCoins(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String packId = reqString(args, "packId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        long coins = packCoins(packId);
        if (coins < 0) {
            throw new ApiException("That coin pack doesn't exist.");
        }
        // No real payment inside this app: packs credit instantly, like the
        // web version, and say so in the UI.
        account.put("coins", coinsOf(account) + coins);
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("id", nextRowId(paymentHistory));
        h.put("accountId", accountId);
        h.put("kind", "coins");
        h.put("description", "Bought " + coins + " coins");
        h.put("amountCents", packCents(packId));
        h.put("status", "completed");
        h.put("createdAt", nowIso());
        paymentHistory.add(h);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("coins", coinsOf(account));
        return resp;
    }

    private Object sendGift(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String giftId = reqString(args, "giftId");
        Map<String, Object> sender = accountById(accountId);
        if (sender == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> gift = null;
        for (Map<String, Object> g : giftCatalog()) {
            if (giftId.equals(str(g.get("id"), ""))) {
                gift = g;
                break;
            }
        }
        if (gift == null) {
            throw new ApiException("That gift doesn't exist.");
        }
        Long receiverId = args.get("receiverId") instanceof Number
                ? ((Number) args.get("receiverId")).longValue() : null;
        Long postId = args.get("postId") instanceof Number
                ? ((Number) args.get("postId")).longValue() : null;
        Long liveSessionId = args.get("liveSessionId") instanceof Number
                ? ((Number) args.get("liveSessionId")).longValue() : null;
        if (receiverId == null && postId != null) {
            Map<String, Object> p = postById(postId);
            if (p != null) {
                receiverId = num(p.get("accountId"), -1);
            }
        }
        if (receiverId == null && liveSessionId != null) {
            for (Map<String, Object> s : liveSessions) {
                if (num(s.get("id"), -1) == liveSessionId) {
                    receiverId = num(s.get("hostId"), -1);
                    break;
                }
            }
        }
        if (receiverId == null) {
            throw new ApiException("Could not find who receives this gift.");
        }
        if (receiverId == accountId) {
            throw new ApiException("You can't send a gift to yourself.");
        }
        Map<String, Object> receiver = accountById(receiverId);
        if (receiver == null) {
            throw new ApiException("Could not find who receives this gift.");
        }
        if (isBlockedEitherWay(accountId, receiverId)) {
            throw new ApiException("You can't send a gift to this account.");
        }
        long cost = num(gift.get("coins"), 0);
        if (coinsOf(sender) < cost) {
            throw new ApiException("Not enough coins \u2014 get more coins first.");
        }
        // The creator earns diamonds worth half the coins.
        long diamonds = Math.round(cost / 2.0);
        sender.put("coins", coinsOf(sender) - cost);
        receiver.put("diamonds", diamondsOf(receiver) + diamonds);
        Map<String, Object> tx = new LinkedHashMap<>();
        tx.put("id", nextRowId(giftTx));
        tx.put("senderId", accountId);
        tx.put("receiverId", receiverId);
        tx.put("giftId", giftId);
        tx.put("giftName", str(gift.get("name"), ""));
        tx.put("coins", cost);
        tx.put("diamonds", diamonds);
        tx.put("postId", postId);
        tx.put("liveSessionId", liveSessionId);
        tx.put("createdAt", nowIso());
        giftTx.add(tx);
        notify(receiverId, accountId, "gift", postId);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("coins", coinsOf(sender));
        resp.put("diamondsEarned", diamonds);
        return resp;
    }

    private Object getEarnings(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        long totalDiamondsEarned = 0;
        long totalCoinsSpent = 0;
        List<Object> received = new ArrayList<>();
        List<Object> sent = new ArrayList<>();
        for (Map<String, Object> tx : giftTx) {
            if (num(tx.get("receiverId"), -1) == accountId) {
                totalDiamondsEarned += num(tx.get("diamonds"), 0);
                received.add(giftTxJson(tx));
            }
            if (num(tx.get("senderId"), -1) == accountId) {
                totalCoinsSpent += num(tx.get("coins"), 0);
                sent.add(giftTxJson(tx));
            }
        }
        List<Object> payoutJson = new ArrayList<>();
        for (Map<String, Object> p : payouts) {
            if (num(p.get("accountId"), -1) == accountId) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", num(p.get("id"), 0));
                item.put("diamonds", num(p.get("diamonds"), 0));
                item.put("amountCents", num(p.get("amountCents"), 0));
                item.put("method", str(p.get("method"), ""));
                item.put("status", str(p.get("status"), ""));
                item.put("createdAt", str(p.get("createdAt"), ""));
                payoutJson.add(item);
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("coins", coinsOf(account));
        resp.put("diamonds", diamondsOf(account));
        resp.put("diamondsCents", Math.round(diamondsOf(account) * 0.5));
        resp.put("totalDiamondsEarned", totalDiamondsEarned);
        resp.put("totalCoinsSpent", totalCoinsSpent);
        resp.put("received", received);
        resp.put("sent", sent);
        resp.put("payouts", payoutJson);
        return resp;
    }

    private Object addPaymentMethod(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String type = reqString(args, "type");
        String label = reqString(args, "label").trim();
        String last4 = reqString(args, "last4").trim();
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (!"card".equals(type) && !"bank".equals(type)
                && !"paypal".equals(type) && !"bkash".equals(type)) {
            throw new ApiException("Unknown payment method type.");
        }
        if (label.length() < 2 || label.length() > 60) {
            throw new ApiException("Label must be 2-60 characters.");
        }
        if (!last4.matches("^\\d{4}$")) {
            throw new ApiException("Enter exactly the last 4 digits.");
        }
        boolean first = true;
        for (Map<String, Object> m : paymentMethods) {
            if (num(m.get("accountId"), -1) == accountId) {
                first = false;
                break;
            }
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(paymentMethods));
        row.put("accountId", accountId);
        row.put("type", type);
        row.put("label", label);
        row.put("last4", last4);
        row.put("isDefault", first ? 1L : 0L);
        row.put("createdAt", nowIso());
        paymentMethods.add(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object removePaymentMethod(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long methodId = reqLong(args, "methodId");
        Map<String, Object> row = null;
        for (Map<String, Object> m : paymentMethods) {
            if (num(m.get("id"), -1) == methodId) {
                row = m;
                break;
            }
        }
        if (row == null) {
            throw new ApiException("That payment method no longer exists.");
        }
        if (num(row.get("accountId"), -1) != accountId) {
            throw new ApiException("That payment method isn't yours.");
        }
        boolean wasDefault = num(row.get("isDefault"), 0) == 1;
        paymentMethods.remove(row);
        if (wasDefault) {
            for (Map<String, Object> m : paymentMethods) {
                if (num(m.get("accountId"), -1) == accountId) {
                    m.put("isDefault", 1L);
                    break;
                }
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object setDefaultPaymentMethod(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long methodId = reqLong(args, "methodId");
        Map<String, Object> row = null;
        for (Map<String, Object> m : paymentMethods) {
            if (num(m.get("id"), -1) == methodId) {
                row = m;
                break;
            }
        }
        if (row == null) {
            throw new ApiException("That payment method no longer exists.");
        }
        if (num(row.get("accountId"), -1) != accountId) {
            throw new ApiException("That payment method isn't yours.");
        }
        for (Map<String, Object> m : paymentMethods) {
            if (num(m.get("accountId"), -1) == accountId) {
                m.put("isDefault", num(m.get("id"), 0) == methodId ? 1L : 0L);
            }
        }
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> taxRow(long accountId) {
        Object v = taxProfiles.get(String.valueOf(accountId));
        if (v instanceof Map) {
            return (Map<String, Object>) v;
        }
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("legalName", "");
        t.put("country", "");
        t.put("taxIdLast4", "");
        t.put("formType", "W-9");
        taxProfiles.put(String.valueOf(accountId), t);
        return t;
    }

    private Object updateTaxProfile(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> t = taxRow(accountId);
        t.put("legalName", str(args.get("legalName"), "").trim());
        t.put("country", str(args.get("country"), "").trim());
        t.put("taxIdLast4", str(args.get("taxIdLast4"), "").trim());
        String formType = str(args.get("formType"), "W-9").trim();
        t.put("formType", formType.isEmpty() ? "W-9" : formType);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object getPaymentsOverview(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        List<Object> methods = new ArrayList<>();
        for (Map<String, Object> m : paymentMethods) {
            if (num(m.get("accountId"), -1) == accountId) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", num(m.get("id"), 0));
                item.put("type", str(m.get("type"), ""));
                item.put("label", str(m.get("label"), ""));
                item.put("last4", str(m.get("last4"), ""));
                item.put("isDefault", num(m.get("isDefault"), 0) == 1);
                item.put("createdAt", str(m.get("createdAt"), ""));
                methods.add(item);
            }
        }
        Map<String, Object> t = taxRow(accountId);
        Map<String, Object> tax = new LinkedHashMap<>();
        tax.put("legalName", str(t.get("legalName"), ""));
        tax.put("country", str(t.get("country"), ""));
        tax.put("taxIdLast4", str(t.get("taxIdLast4"), ""));
        tax.put("formType", str(t.get("formType"), "W-9"));
        List<Object> history = new ArrayList<>();
        for (Map<String, Object> h : paymentHistory) {
            if (num(h.get("accountId"), -1) == accountId) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", num(h.get("id"), 0));
                item.put("kind", str(h.get("kind"), ""));
                item.put("description", str(h.get("description"), ""));
                item.put("amountCents", num(h.get("amountCents"), 0));
                item.put("status", str(h.get("status"), ""));
                item.put("createdAt", str(h.get("createdAt"), ""));
                history.add(item);
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("methods", methods);
        resp.put("tax", tax);
        resp.put("history", history);
        return resp;
    }

    private Object requestPayout(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String method = reqString(args, "method").trim();
        String note = str(args.get("note"), "").trim();
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (method.length() < 2 || method.length() > 80) {
            throw new ApiException("Enter a payout method.");
        }
        long diamonds = diamondsOf(account);
        if (diamonds < 100) {
            throw new ApiException("You need at least 100 diamonds to request a payout.");
        }
        long amountCents = Math.round(diamonds * 0.5);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(payouts));
        row.put("accountId", accountId);
        row.put("diamonds", diamonds);
        row.put("amountCents", amountCents);
        row.put("method", method);
        row.put("note", note);
        row.put("status", "pending");
        row.put("createdAt", nowIso());
        payouts.add(row);
        account.put("diamonds", 0L);
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("id", nextRowId(paymentHistory));
        h.put("accountId", accountId);
        h.put("kind", "payout");
        h.put("description", "Payout request (" + diamonds + " diamonds)");
        h.put("amountCents", amountCents);
        h.put("status", "pending");
        h.put("createdAt", nowIso());
        paymentHistory.add(h);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    // ------------------------------------------------------------------
    // LIVE (simulated: no real broadcast media server on the phone)
    // ------------------------------------------------------------------

    private Map<String, Object> liveSessionById(long id) {
        for (Map<String, Object> s : liveSessions) {
            if (num(s.get("id"), -1) == id) {
                return s;
            }
        }
        return null;
    }

    private Map<String, Object> hydrateLive(Map<String, Object> row) {
        Map<String, Object> host = accountById(num(row.get("hostId"), -1));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", num(row.get("id"), 0));
        m.put("title", str(row.get("title"), ""));
        m.put("coverText", str(row.get("coverText"), ""));
        m.put("isCamera", num(row.get("isCamera"), 0) == 1);
        m.put("simulated", true);
        m.put("status", str(row.get("status"), "ended"));
        m.put("viewerCount", countWhere(liveViewers, "sessionId", num(row.get("id"), 0)));
        m.put("createdAt", str(row.get("createdAt"), ""));
        m.put("host", host == null ? personJson(new LinkedHashMap<>()) : personJson(host));
        Object bpid = row.get("broadcastPostId");
        if (bpid instanceof Number) {
            Map<String, Object> p = postById(((Number) bpid).longValue());
            if (p != null) {
                Map<String, Object> b = new LinkedHashMap<>();
                b.put("postId", num(p.get("id"), 0));
                b.put("mediaUrl", "/media/" + str(p.get("mediaBlobKey"), ""));
                b.put("kind", str(p.get("kind"), ""));
                m.put("broadcast", b);
            } else {
                m.put("broadcast", null);
            }
        } else {
            m.put("broadcast", null);
        }
        return m;
    }

    private Object startLive(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String title = str(args.get("title"), "").trim();
        String coverText = str(args.get("coverText"), "").trim();
        boolean isCamera = bool(args.get("isCamera"), false);
        Long broadcastPostId = args.get("broadcastPostId") instanceof Number
                ? ((Number) args.get("broadcastPostId")).longValue() : null;
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (broadcastPostId != null) {
            Map<String, Object> p = postById(broadcastPostId);
            if (p == null || num(p.get("accountId"), -1) != accountId) {
                throw new ApiException("That post can't be broadcast.");
            }
        }
        for (Map<String, Object> s : liveSessions) {
            if (num(s.get("hostId"), -1) == accountId
                    && "live".equals(str(s.get("status"), ""))) {
                s.put("status", "ended");
            }
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(liveSessions));
        row.put("hostId", accountId);
        row.put("title", title);
        row.put("coverText", coverText);
        row.put("isCamera", isCamera ? 1L : 0L);
        row.put("status", "live");
        row.put("broadcastPostId", broadcastPostId);
        row.put("createdAt", nowIso());
        liveSessions.add(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("session", hydrateLive(row));
        return resp;
    }

    private Object endLive(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long sessionId = reqLong(args, "sessionId");
        Map<String, Object> session = liveSessionById(sessionId);
        if (session == null) {
            throw new ApiException("That LIVE room no longer exists.");
        }
        if (num(session.get("hostId"), -1) != accountId) {
            throw new ApiException("Only the host can end this LIVE.");
        }
        session.put("status", "ended");
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> v : liveViewers) {
            if (num(v.get("sessionId"), -1) == sessionId) {
                dead.add(v);
            }
        }
        liveViewers.removeAll(dead);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object listLive(Map<String, Object> args) {
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> s : liveSessions) {
            if ("live".equals(str(s.get("status"), ""))) {
                out.add(hydrateLive(s));
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("sessions", out);
        return resp;
    }

    private Map<String, Object> liveMessageJson(Map<String, Object> m) {
        Map<String, Object> sender = accountById(num(m.get("senderId"), -1));
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", num(m.get("id"), 0));
        item.put("body", str(m.get("body"), ""));
        item.put("createdAt", str(m.get("createdAt"), ""));
        item.put("sender", sender == null ? personJson(new LinkedHashMap<>()) : personJson(sender));
        return item;
    }

    private Object joinLive(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long sessionId = reqLong(args, "sessionId");
        Map<String, Object> session = liveSessionById(sessionId);
        if (session == null || !"live".equals(str(session.get("status"), ""))) {
            throw new ApiException("That LIVE room is no longer live.");
        }
        boolean present = false;
        for (Map<String, Object> v : liveViewers) {
            if (num(v.get("sessionId"), -1) == sessionId
                    && num(v.get("accountId"), -1) == accountId) {
                present = true;
                break;
            }
        }
        if (!present) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("id", nextRowId(liveViewers));
            v.put("sessionId", sessionId);
            v.put("accountId", accountId);
            v.put("createdAt", nowIso());
            liveViewers.add(v);
            saveAll();
        }
        List<Object> msgs = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> m : liveMessages) {
            if (num(m.get("sessionId"), -1) == sessionId) {
                rows.add(m);
            }
        }
        rows.sort((a, b) -> Long.compare(num(a.get("id"), 0), num(b.get("id"), 0)));
        if (rows.size() > 50) {
            rows = new ArrayList<>(rows.subList(rows.size() - 50, rows.size()));
        }
        for (Map<String, Object> m : rows) {
            msgs.add(liveMessageJson(m));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("session", hydrateLive(session));
        resp.put("messages", msgs);
        return resp;
    }

    private Object leaveLive(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long sessionId = reqLong(args, "sessionId");
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> v : liveViewers) {
            if (num(v.get("sessionId"), -1) == sessionId
                    && num(v.get("accountId"), -1) == accountId) {
                dead.add(v);
            }
        }
        liveViewers.removeAll(dead);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object listLiveMessages(Map<String, Object> args) throws ApiException {
        long sessionId = reqLong(args, "sessionId");
        long afterId = num(args.get("afterId"), 0);
        Map<String, Object> session = liveSessionById(sessionId);
        if (session == null) {
            throw new ApiException("That LIVE room no longer exists.");
        }
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> m : liveMessages) {
            if (num(m.get("sessionId"), -1) == sessionId
                    && num(m.get("id"), 0) > afterId) {
                out.add(liveMessageJson(m));
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("messages", out);
        resp.put("viewerCount", countWhere(liveViewers, "sessionId", sessionId));
        resp.put("status", str(session.get("status"), "ended"));
        return resp;
    }

    private Object sendLiveMessage(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long sessionId = reqLong(args, "sessionId");
        String body = reqString(args, "body").trim();
        if (body.length() < 1 || body.length() > 300) {
            throw new ApiException("Message must be 1-300 characters.");
        }
        Map<String, Object> session = liveSessionById(sessionId);
        if (session == null || !"live".equals(str(session.get("status"), ""))) {
            throw new ApiException("That LIVE room is no longer live.");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(liveMessages));
        row.put("sessionId", sessionId);
        row.put("senderId", accountId);
        row.put("body", body);
        row.put("createdAt", nowIso());
        liveMessages.add(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object addLiveModerator(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long sessionId = reqLong(args, "sessionId");
        String username = reqString(args, "username").trim().toLowerCase();
        Map<String, Object> session = liveSessionById(sessionId);
        if (session == null) {
            throw new ApiException("That LIVE room no longer exists.");
        }
        if (num(session.get("hostId"), -1) != accountId) {
            throw new ApiException("Only the host can add moderators.");
        }
        Map<String, Object> target = null;
        for (Map<String, Object> a : accounts) {
            if (str(a.get("username"), "").equals(username)) {
                target = a;
                break;
            }
        }
        if (target == null) {
            throw new ApiException("No account with that username.");
        }
        long targetId = num(target.get("id"), 0);
        boolean present = false;
        for (Map<String, Object> m : liveModerators) {
            if (num(m.get("sessionId"), -1) == sessionId
                    && num(m.get("accountId"), -1) == targetId) {
                present = true;
                break;
            }
        }
        if (!present) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextRowId(liveModerators));
            row.put("sessionId", sessionId);
            row.put("accountId", targetId);
            row.put("createdAt", nowIso());
            liveModerators.add(row);
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object removeLiveModerator(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long sessionId = reqLong(args, "sessionId");
        long moderatorId = reqLong(args, "moderatorId");
        Map<String, Object> session = liveSessionById(sessionId);
        if (session == null) {
            throw new ApiException("That LIVE room no longer exists.");
        }
        if (num(session.get("hostId"), -1) != accountId) {
            throw new ApiException("Only the host can remove moderators.");
        }
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> m : liveModerators) {
            if (num(m.get("sessionId"), -1) == sessionId
                    && num(m.get("accountId"), -1) == moderatorId) {
                dead.add(m);
            }
        }
        liveModerators.removeAll(dead);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object listLiveModerators(Map<String, Object> args) throws ApiException {
        long sessionId = reqLong(args, "sessionId");
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> m : liveModerators) {
            if (num(m.get("sessionId"), -1) == sessionId) {
                Map<String, Object> a = accountById(num(m.get("accountId"), -1));
                if (a != null) {
                    out.add(personJson(a));
                }
            }
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("moderators", out);
        return resp;
    }

    // ------------------------------------------------------------------
    // reports and admin
    // ------------------------------------------------------------------

    private Object createReport(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String targetType = reqString(args, "targetType");
        long targetId = reqLong(args, "targetId");
        String reason = str(args.get("reason"), "").trim();
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (!"post".equals(targetType) && !"account".equals(targetType)
                && !"comment".equals(targetType) && !"live".equals(targetType)) {
            throw new ApiException("Unknown report target.");
        }
        if (reason.length() > 200) {
            throw new ApiException("Reason must be 200 characters or less.");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", nextRowId(reports));
        row.put("accountId", accountId);
        row.put("targetType", targetType);
        row.put("targetId", targetId);
        row.put("reason", reason);
        row.put("status", "open");
        row.put("createdAt", nowIso());
        reports.add(row);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private String reportTargetLabel(Map<String, Object> report) {
        String type = str(report.get("targetType"), "");
        long targetId = num(report.get("targetId"), 0);
        if ("post".equals(type)) {
            Map<String, Object> p = postById(targetId);
            if (p != null) {
                String caption = str(p.get("caption"), "");
                return caption.length() > 60 ? caption.substring(0, 60) : caption;
            }
            return "Post #" + targetId;
        }
        if ("account".equals(type)) {
            Map<String, Object> a = accountById(targetId);
            return a == null ? "Account #" + targetId : "@" + str(a.get("username"), "");
        }
        if ("comment".equals(type)) {
            Map<String, Object> c = commentById(targetId);
            if (c != null) {
                String text = str(c.get("text"), "");
                return text.length() > 60 ? text.substring(0, 60) : text;
            }
            return "Comment #" + targetId;
        }
        if ("live".equals(type)) {
            Map<String, Object> s = liveSessionById(targetId);
            return s == null ? "LIVE #" + targetId : str(s.get("title"), "LIVE");
        }
        return "#" + targetId;
    }

    private Object adminListReports(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        requireAdmin(adminId);
        List<Map<String, Object>> rows = new ArrayList<>(reports);
        rows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> reporter = accountById(num(r.get("accountId"), -1));
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", num(r.get("id"), 0));
            item.put("targetType", str(r.get("targetType"), ""));
            item.put("targetId", num(r.get("targetId"), 0));
            item.put("reason", str(r.get("reason"), ""));
            item.put("status", str(r.get("status"), ""));
            item.put("createdAt", str(r.get("createdAt"), ""));
            item.put("reporter", reporter == null
                    ? personJson(new LinkedHashMap<>()) : personJson(reporter));
            item.put("targetLabel", reportTargetLabel(r));
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("reports", out);
        return resp;
    }

    private Object adminResolveReport(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        long reportId = reqLong(args, "reportId");
        String status = reqString(args, "status");
        requireAdmin(adminId);
        if (!"resolved".equals(status) && !"dismissed".equals(status)) {
            throw new ApiException("Unknown report status.");
        }
        Map<String, Object> report = null;
        for (Map<String, Object> r : reports) {
            if (num(r.get("id"), -1) == reportId) {
                report = r;
                break;
            }
        }
        if (report == null) {
            throw new ApiException("That report no longer exists.");
        }
        report.put("status", status);
        logAdmin(adminId, "report_" + status, "report", reportId,
                reportTargetLabel(report));
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object adminDeleteComment(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        long commentId = reqLong(args, "commentId");
        requireAdmin(adminId);
        Map<String, Object> comment = commentById(commentId);
        if (comment == null) {
            throw new ApiException("That comment no longer exists.");
        }
        String text = str(comment.get("text"), "");
        comments.remove(comment);
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> r : commentLikes) {
            if (num(r.get("commentId"), -1) == commentId) {
                dead.add(r);
            }
        }
        commentLikes.removeAll(dead);
        logAdmin(adminId, "comment_deleted", "comment", commentId, text);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object adminSuspendAccount(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        long targetId = reqLong(args, "targetId");
        boolean suspended = bool(args.get("suspended"), false);
        requireAdmin(adminId);
        Map<String, Object> target = accountById(targetId);
        if (target == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (num(target.get("isAdmin"), 0) == 1) {
            throw new ApiException("Admin accounts can't be suspended.");
        }
        target.put("suspended", suspended ? 1L : 0L);
        logAdmin(adminId, suspended ? "account_suspended" : "account_unsuspended",
                "account", targetId, str(target.get("username"), ""));
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object adminListAuditLog(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        requireAdmin(adminId);
        List<Map<String, Object>> rows = new ArrayList<>(auditLog);
        rows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        if (rows.size() > 200) {
            rows = new ArrayList<>(rows.subList(0, 200));
        }
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> admin = accountById(num(r.get("adminId"), -1));
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", num(r.get("id"), 0));
            item.put("action", str(r.get("action"), ""));
            item.put("targetType", str(r.get("targetType"), ""));
            Object tid = r.get("targetId");
            item.put("targetId", tid instanceof Number ? ((Number) tid).longValue() : null);
            item.put("detail", str(r.get("detail"), ""));
            item.put("createdAt", str(r.get("createdAt"), ""));
            Map<String, Object> adminJson = new LinkedHashMap<>();
            adminJson.put("id", admin == null ? 0 : num(admin.get("id"), 0));
            adminJson.put("username", admin == null ? "" : str(admin.get("username"), ""));
            item.put("admin", adminJson);
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("entries", out);
        return resp;
    }

    private Object adminListPayouts(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        requireAdmin(adminId);
        List<Map<String, Object>> rows = new ArrayList<>(payouts);
        rows.sort((a, b) -> Long.compare(num(b.get("id"), 0), num(a.get("id"), 0)));
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> p : rows) {
            Map<String, Object> account = accountById(num(p.get("accountId"), -1));
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", num(p.get("id"), 0));
            item.put("diamonds", num(p.get("diamonds"), 0));
            item.put("amountCents", num(p.get("amountCents"), 0));
            item.put("method", str(p.get("method"), ""));
            item.put("note", str(p.get("note"), ""));
            item.put("status", str(p.get("status"), ""));
            item.put("createdAt", str(p.get("createdAt"), ""));
            Map<String, Object> accJson = new LinkedHashMap<>();
            accJson.put("id", account == null ? 0 : num(account.get("id"), 0));
            accJson.put("username", account == null ? "" : str(account.get("username"), ""));
            accJson.put("displayName", account == null ? "" : str(account.get("displayName"), ""));
            item.put("account", accJson);
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("payouts", out);
        return resp;
    }

    private Object adminSetPayoutStatus(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        long payoutId = reqLong(args, "payoutId");
        String status = reqString(args, "status");
        requireAdmin(adminId);
        if (!"paid".equals(status) && !"rejected".equals(status)) {
            throw new ApiException("Unknown payout status.");
        }
        Map<String, Object> payout = null;
        for (Map<String, Object> p : payouts) {
            if (num(p.get("id"), -1) == payoutId) {
                payout = p;
                break;
            }
        }
        if (payout == null) {
            throw new ApiException("That payout no longer exists.");
        }
        if (!"pending".equals(str(payout.get("status"), ""))) {
            throw new ApiException("That payout was already decided.");
        }
        payout.put("status", status);
        if ("rejected".equals(status)) {
            Map<String, Object> account = accountById(num(payout.get("accountId"), -1));
            if (account != null) {
                account.put("diamonds",
                        diamondsOf(account) + num(payout.get("diamonds"), 0));
            }
        }
        logAdmin(adminId, "payout_" + status, "payout", payoutId,
                str(payout.get("method"), ""));
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object adminBroadcastNotification(Map<String, Object> args) throws ApiException {
        long adminId = reqLong(args, "adminId");
        String message = reqString(args, "message").trim();
        requireAdmin(adminId);
        if (message.length() < 1 || message.length() > 500) {
            throw new ApiException("Message must be 1-500 characters.");
        }
        long sentTo = 0;
        for (Map<String, Object> a : accounts) {
            long id = num(a.get("id"), 0);
            if (id == adminId) {
                continue;
            }
            notifyBody(id, adminId, "system", null, message);
            sentTo++;
        }
        logAdmin(adminId, "broadcast_sent", "account", null,
                message.length() > 120 ? message.substring(0, 120) : message);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        resp.put("sentTo", sentTo);
        return resp;
    }

    // ------------------------------------------------------------------
    // auth security, verification codes, OAuth status
    // ------------------------------------------------------------------

    private Object getAuthSecurity(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("security", securityOf(account));
        return resp;
    }

    private Object setPassword(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        String newPassword = reqString(args, "newPassword");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (newPassword.length() < 6 || newPassword.length() > 72) {
            throw new ApiException("Password must be 6-72 characters.");
        }
        account.put("password", newPassword);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object setTwoFactor(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        boolean enabled = bool(args.get("enabled"), false);
        String code = str(args.get("code"), "");
        Map<String, Object> account = accountById(accountId);
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (enabled) {
            if (!consumeCode(accountId, "enable_2fa", code)) {
                throw new ApiException("Enter the demo code first \u2014 two-factor turns on only after the code checks out.");
            }
        }
        account.put("twoFactorEnabled", enabled ? 1L : 0L);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("security", securityOf(account));
        return resp;
    }

    private static boolean isCodePurpose(String p) {
        return "verify_email".equals(p) || "verify_phone".equals(p)
                || "login_2fa".equals(p) || "enable_2fa".equals(p)
                || "password_reset".equals(p) || "signup".equals(p);
    }

    private Object requestVerificationCode(Map<String, Object> args) throws ApiException {
        String purpose = reqString(args, "purpose");
        if (!isCodePurpose(purpose)) {
            throw new ApiException("Unknown verification purpose.");
        }
        String identifier = args.get("identifier") instanceof String
                ? ((String) args.get("identifier")).trim() : "";
        Long accountId = args.get("accountId") instanceof Number
                ? ((Number) args.get("accountId")).longValue() : null;
        // No real SMS provider is connected: the code is a DEMO code shown
        // inside the app. It really is stored here and really is checked
        // by the verify/reset actions.
        if ("signup".equals(purpose)) {
            if (identifier.isEmpty()) {
                throw new ApiException("Enter your phone number first.");
            }
            String norm = identifier.contains("@")
                    ? normalizeEmail(identifier) : normalizePhone(identifier);
            if (norm == null || norm.length() < 7) {
                throw new ApiException("That phone number doesn't look right \u2014 check it and try again.");
            }
            if (findAccountByIdentifier(identifier) != null) {
                throw new ApiException("That phone number or email is already registered \u2014 log in instead.");
            }
            String code = issueCode(0, norm, purpose);
            saveAll();
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("demoCode", code);
            resp.put("expiresInMinutes", 10L);
            resp.put("sentTo", norm);
            resp.put("demo", true);
            return resp;
        }
        Map<String, Object> account = null;
        if (accountId != null) {
            account = accountById(accountId);
        } else if (!identifier.isEmpty()) {
            account = findAccountByIdentifier(identifier);
        }
        if (account == null) {
            throw new ApiException("That account no longer exists.");
        }
        String sentTo = identifier;
        if (sentTo.isEmpty()) {
            if ("verify_email".equals(purpose)) {
                sentTo = str(account.get("email"), "");
            } else {
                sentTo = str(account.get("phone"), "");
            }
        }
        if (sentTo.isEmpty()) {
            throw new ApiException("Add a phone number or email first.");
        }
        String norm = sentTo.contains("@") ? normalizeEmail(sentTo) : normalizePhone(sentTo);
        String code = issueCode(num(account.get("id"), 0), norm == null ? sentTo : norm, purpose);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("demoCode", code);
        resp.put("expiresInMinutes", 10L);
        resp.put("sentTo", sentTo);
        resp.put("demo", true);
        return resp;
    }

    private Object verifyCode(Map<String, Object> args) throws ApiException {
        String purpose = reqString(args, "purpose");
        String code = reqString(args, "code");
        if (!isCodePurpose(purpose)) {
            throw new ApiException("Unknown verification purpose.");
        }
        Long accountId = args.get("accountId") instanceof Number
                ? ((Number) args.get("accountId")).longValue() : null;
        String identifier = args.get("identifier") instanceof String
                ? ((String) args.get("identifier")).trim() : "";
        Map<String, Object> account = null;
        boolean ok;
        if (accountId != null) {
            account = accountById(accountId);
            if (account == null) {
                throw new ApiException("That account no longer exists.");
            }
            ok = consumeCode(accountId, purpose, code);
        } else if (!identifier.isEmpty()) {
            String norm = identifier.contains("@")
                    ? normalizeEmail(identifier) : normalizePhone(identifier);
            ok = consumeCodeByIdentifier(norm == null ? identifier : norm, purpose, code);
            if (ok) {
                account = findAccountByIdentifier(identifier);
            }
        } else {
            throw new ApiException("Enter the code first.");
        }
        if (!ok) {
            throw new ApiException("That code is wrong or expired \u2014 request a new one.");
        }
        if (account != null) {
            if ("verify_email".equals(purpose)) {
                account.put("emailVerified", 1L);
            } else if ("verify_phone".equals(purpose)) {
                account.put("phoneVerified", 1L);
            }
            saveAll();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        resp.put("accountId", account == null ? 0L : num(account.get("id"), 0));
        return resp;
    }

    private Object resetPasswordWithCode(Map<String, Object> args) throws ApiException {
        String identifier = reqString(args, "identifier").trim();
        String code = reqString(args, "code");
        String newPassword = reqString(args, "newPassword");
        if (newPassword.length() < 6 || newPassword.length() > 72) {
            throw new ApiException("Password must be 6-72 characters.");
        }
        String norm = identifier.contains("@")
                ? normalizeEmail(identifier) : normalizePhone(identifier);
        if (!consumeCodeByIdentifier(norm == null ? identifier : norm,
                "password_reset", code)) {
            throw new ApiException("That code is wrong or expired \u2014 request a new one.");
        }
        Map<String, Object> account = findAccountByIdentifier(identifier);
        if (account == null) {
            throw new ApiException("No account found for that phone number or email.");
        }
        account.put("password", newPassword);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        resp.put("accountId", num(account.get("id"), 0));
        return resp;
    }

    private Object getOAuthStatus(Map<String, Object> args) {
        Map<String, Object> google = new LinkedHashMap<>();
        google.put("configured", false);
        google.put("note", "Demo only. Real Google sign-in needs your own Google Cloud project (OAuth client ID) connected here. No account was signed in.");
        Map<String, Object> apple = new LinkedHashMap<>();
        apple.put("configured", false);
        apple.put("note", "Demo only. Real Apple sign-in needs your own Apple Developer account connected here. No account was signed in.");
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("google", google);
        resp.put("apple", apple);
        return resp;
    }

    private Object sendTestPush(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        Map<String, Object> s = settingsRow(accountId);
        boolean delivered = num(s.get("pushEnabled"), 1) == 1;
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        resp.put("delivered", delivered);
        return resp;
    }

    // ------------------------------------------------------------------
    // discovery, creator stats, follow lists
    // ------------------------------------------------------------------

    private Object getTrendingHashtags(Map<String, Object> args) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map<String, Object> p : posts) {
            String caption = str(p.get("caption"), "");
            int i = 0;
            while (i < caption.length()) {
                if (caption.charAt(i) == '#') {
                    int j = i + 1;
                    while (j < caption.length()) {
                        char c = caption.charAt(j);
                        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                                || (c >= '0' && c <= '9') || c == '_') {
                            j++;
                        } else {
                            break;
                        }
                    }
                    if (j > i + 1) {
                        String tag = caption.substring(i + 1, j).toLowerCase();
                        counts.put(tag, counts.getOrDefault(tag, 0) + 1);
                    }
                    i = j;
                } else {
                    i++;
                }
            }
        }
        List<Map.Entry<String, Integer>> sorted = new ArrayList<>(counts.entrySet());
        sorted.sort((a, b) -> b.getValue() - a.getValue());
        List<Object> out = new ArrayList<>();
        for (int i = 0; i < sorted.size() && i < 20; i++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("tag", sorted.get(i).getKey());
            item.put("count", (long) sorted.get(i).getValue());
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("hashtags", out);
        return resp;
    }

    private Object getCreatorStats(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        long totalViews = 0;
        long totalLikes = 0;
        long totalComments = 0;
        long totalShares = 0;
        long watchSecondsTotal = 0;
        long watchEvents = 0;
        long completedTotal = 0;
        List<Object> videos = new ArrayList<>();
        for (Map<String, Object> p : posts) {
            if (num(p.get("accountId"), -1) != accountId) {
                continue;
            }
            long pid = num(p.get("id"), 0);
            long likeCount = countWhere(likes, "postId", pid);
            long commentCount = countWhere(comments, "postId", pid);
            long views = num(p.get("viewCount"), 0);
            long shares = num(p.get("shareCount"), 0);
            totalViews += views;
            totalLikes += likeCount;
            totalComments += commentCount;
            totalShares += shares;
            long seconds = 0;
            long events = 0;
            long completed = 0;
            for (Map<String, Object> w : watches) {
                if (num(w.get("postId"), -1) == pid) {
                    seconds += num(w.get("watchSeconds"), 0);
                    events++;
                    if (num(w.get("completed"), 0) == 1) {
                        completed++;
                    }
                }
            }
            watchSecondsTotal += seconds;
            watchEvents += events;
            completedTotal += completed;
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", pid);
            item.put("caption", str(p.get("caption"), ""));
            item.put("kind", str(p.get("kind"), ""));
            item.put("viewCount", views);
            item.put("likeCount", likeCount);
            item.put("commentCount", commentCount);
            item.put("shareCount", shares);
            item.put("avgWatchSeconds", events == 0 ? 0.0 : (double) seconds / events);
            item.put("completionRate", events == 0 ? 0.0 : (double) completed / events);
            item.put("createdAt", str(p.get("createdAt"), ""));
            videos.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("totalViews", totalViews);
        resp.put("totalLikes", totalLikes);
        resp.put("totalComments", totalComments);
        resp.put("totalShares", totalShares);
        resp.put("followers", countWhere(follows, "followeeId", accountId));
        resp.put("following", countWhere(follows, "followerId", accountId));
        resp.put("avgWatchSeconds", watchEvents == 0 ? 0.0 : (double) watchSecondsTotal / watchEvents);
        resp.put("completionRate", watchEvents == 0 ? 0.0 : (double) completedTotal / watchEvents);
        resp.put("watchEvents", watchEvents);
        resp.put("videos", videos);
        return resp;
    }

    private Object listFollowList(Map<String, Object> args) throws ApiException {
        long viewerId = num(args.get("viewerId"), 0);
        long accountId = reqLong(args, "accountId");
        String kind = reqString(args, "kind");
        if (accountById(accountId) == null) {
            throw new ApiException("That account no longer exists.");
        }
        if (!"followers".equals(kind) && !"following".equals(kind)) {
            throw new ApiException("Unknown follow list.");
        }
        List<Long> ids = new ArrayList<>();
        for (Map<String, Object> f : follows) {
            if ("followers".equals(kind) && num(f.get("followeeId"), -1) == accountId) {
                ids.add(num(f.get("followerId"), 0));
            } else if ("following".equals(kind) && num(f.get("followerId"), -1) == accountId) {
                ids.add(num(f.get("followeeId"), 0));
            }
        }
        List<Object> out = new ArrayList<>();
        for (Long id : ids) {
            Map<String, Object> a = accountById(id);
            if (a == null) {
                continue;
            }
            Map<String, Object> item = toAccount(a);
            item.put("isMutual", isFollowing(accountId, id) && isFollowing(id, accountId));
            item.put("isFollowing", viewerId > 0 && isFollowing(viewerId, id));
            out.add(item);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("accounts", out);
        return resp;
    }

    private Object removeFollower(Map<String, Object> args) throws ApiException {
        long accountId = reqLong(args, "accountId");
        long followerId = reqLong(args, "followerId");
        List<Map<String, Object>> dead = new ArrayList<>();
        for (Map<String, Object> f : follows) {
            if (num(f.get("followerId"), -1) == followerId
                    && num(f.get("followeeId"), -1) == accountId) {
                dead.add(f);
            }
        }
        follows.removeAll(dead);
        saveAll();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("ok", true);
        return resp;
    }

    private Object seedStarterContent(Map<String, Object> args) {
        if (!posts.isEmpty()) {
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("seeded", false);
            resp.put("count", 0L);
            return resp;
        }
        seedIfEmpty();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("seeded", !posts.isEmpty());
        resp.put("count", (long) posts.size());
        return resp;
    }

    private synchronized void seedIfEmpty() {
        if (!posts.isEmpty()) {
            return;
        }
        Map<String, Object> account = null;
        for (Map<String, Object> a : accounts) {
            if ("clippro".equals(str(a.get("username"), ""))) {
                account = a;
                break;
            }
        }
        if (account == null) {
            account = new LinkedHashMap<>();
            account.put("id", nextAccountId++);
            account.put("username", "clippro");
            account.put("displayName", "ClipPro");
            account.put("bio", "Official ClipPro starter clips \u2014 openly licensed samples so the feed is alive from the first open.");
            account.put("isPrivate", 0L);
            account.put("isAdmin", 1L);
            account.put("dateOfBirth", null);
            account.put("region", "");
            account.put("createdAt", nowIso());
            accounts.add(account);
            settingsRow(num(account.get("id"), 0));
        }
        long accountId = num(account.get("id"), 0);
        String[][] sources = {
                {"starter/starter-1.mp4", "video", "video/mp4", "mp4",
                        "Big Buck Bunny's morning stretch \uD83D\uDC30 Starter clip from the Blender open movie", "Comedy"},
                {"starter/starter-2.mp4", "video", "video/mp4", "mp4",
                        "A flower blooming in fast motion \uD83C\uDF38 CC0 starter clip", "Nature"},
                {"starter/starter-3.mp4", "video", "video/mp4", "mp4",
                        "Sintel crosses the mountains \uD83C\uDFD4\uFE0F Starter clip from the Blender open movie", "Travel"},
                {"starter/starter-4.jpg", "photo", "image/jpeg", "jpg",
                        "A river winding through the valley \u2014 starter photo", "Nature"},
                {"starter/starter-5.jpg", "photo", "image/jpeg", "jpg",
                        "Puppy break \uD83D\uDC36 Starter photo", "Pets"},
                {"starter/starter-6.jpg", "photo", "image/jpeg", "jpg",
                        "Canyon light at golden hour \u2014 starter photo", "Travel"},
        };
        for (int i = 0; i < sources.length; i++) {
            String[] src = sources[i];
            byte[] bytes = assets.read(src[0]);
            if (bytes == null || bytes.length == 0) {
                continue;
            }
            String key = "posts/" + accountId + "/starter-" + (i + 1) + "." + src[3];
            File f = new File(mediaDir, key);
            try {
                if (!f.getParentFile().mkdirs() && !f.getParentFile().isDirectory()) {
                    continue;
                }
                Files.write(f.toPath(), bytes);
            } catch (IOException e) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", nextPostId++);
            row.put("accountId", accountId);
            row.put("kind", src[1]);
            row.put("mediaBlobKey", key);
            row.put("mimeType", src[2]);
            row.put("caption", src[4]);
            row.put("category", src[5]);
            row.put("viewCount", 0L);
            row.put("createdAt", nowIso());
            posts.add(row);
        }
        saveAll();
    }
}
