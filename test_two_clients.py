#!/usr/bin/env python3
"""Two-client verification for the ClipPro shared backend.

Simulates two phones (client A and client B) talking to the SAME backend
URL (default http://127.0.0.1:8080, or pass a public URL as argv[1]).
Checks: account created on A is visible/log-in-able from B; a post made on
A appears in B's For You feed; a like + comment + follow from B sync back
to A; the @clippro admin signs in; media bytes are served.

Usage: python3 test_two_clients.py [base_url]
Exit 0 = all checks passed.
"""
import base64
import json
import sys
import urllib.request
import urllib.error

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8080"
PASS = 0
FAIL = 0


def check(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  PASS  {name}" + (f"  ({detail})" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f"  ({detail})" if detail else ""))


def call(action, args, headers=None):
    body = json.dumps({"action": action, "args": args}).encode()
    req = urllib.request.Request(BASE + "/actions", data=body,
                                 headers={"Content-Type": "application/json",
                                          **(headers or {})}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {"error": "non-json error body"}


def get(path, headers=None):
    req = urllib.request.Request(BASE + path, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read(), r.headers.get("Content-Type")
    except urllib.error.HTTPError as e:
        return e.code, e.read(), None


def data_of(status, resp, action):
    if status != 200 or "data" not in resp:
        raise SystemExit(f"ABORT: {action} failed: HTTP {status} {resp}")
    return resp["data"]


print(f"Shared backend under test: {BASE}")

# 0. Health / mode
status, body, _ = get("/clippro-shared")
mode = json.loads(body.decode()).get("mode") if status == 200 else None
check("backend reports mode=shared", mode == "shared", f"mode={mode}")

# 1. Phone A creates an account
s, r = call("createAccount", {"displayName": "Test Phone A", "username": "phonea",
                              "phone": "+8801700000001", "bio": "shared-server test A"})
d = data_of(s, r, "createAccount(A)")
id_a = d["account"]["id"]
check("phone A creates account 'phonea'", isinstance(id_a, int), f"id={id_a}")

# 2. Phone A posts a photo
with open("/home/hatch/workspace/android-build/ClipPro/app/src/main/assets/starter/starter-5.jpg", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()
s, r = call("createPost", {"accountId": id_a, "kind": "photo",
                           "caption": "Hello from phone A - shared server test",
                           "category": "Pets", "mediaBase64": b64,
                           "mimeType": "image/jpeg"})
d = data_of(s, r, "createPost(A)")
post_id = d["post"]["id"] if "post" in d else d.get("id")
media_url = d.get("mediaUrl") or (d.get("post") or {}).get("mediaUrl")
check("phone A posts a photo", isinstance(post_id, int), f"postId={post_id} mediaUrl={media_url}")

# 3. Media bytes served from the shared backend
if media_url:
    ms, mbody, mtype = get(media_url)
    check("shared backend serves the post media", ms == 200 and len(mbody) > 1000,
          f"HTTP {ms}, {len(mbody)} bytes, {mtype}")

# 4. Phone B creates its own account
s, r = call("createAccount", {"displayName": "Test Phone B", "username": "phoneb",
                              "phone": "+8801700000002", "bio": "shared-server test B"})
d = data_of(s, r, "createAccount(B)")
id_b = d["account"]["id"]
check("phone B creates account 'phoneb'", isinstance(id_b, int), f"id={id_b}")

# 5. Phone B logs in AS phone A's account (cross-phone login)
s, r = call("loginWithContact", {"identifier": "phonea"})
d = data_of(s, r, "loginWithContact(phonea) from B")
check("phone B can log in as phone A's account", d["account"]["id"] == id_a,
      f"id={d['account']['id']}")

# 6. Phone B's For You feed shows A's post
s, r = call("getFeed", {"viewerId": id_b, "tab": "foryou", "limit": 20})
d = data_of(s, r, "getFeed(B)")
posts = d.get("posts", d if isinstance(d, list) else [])
found = [p for p in posts if p.get("id") == post_id]
check("A's post appears in B's For You feed", len(found) == 1,
      f"feed size={len(posts)}")

# 7. Phone B likes, comments, follows
s, r = call("toggleLike", {"accountId": id_b, "postId": post_id})
d = data_of(s, r, "toggleLike(B)")
check("B likes A's post", d.get("liked") is True and d.get("likeCount") == 1,
      f"likeCount={d.get('likeCount')}")
s, r = call("addComment", {"accountId": id_b, "postId": post_id,
                           "text": "Hello from phone B!"})
data_of(s, r, "addComment(B)")
check("B comments on A's post", True)
s, r = call("toggleFollow", {"accountId": id_b, "targetId": id_a})
d = data_of(s, r, "toggleFollow(B->A)")
check("B follows A", d.get("following") is True, f"following={d.get('following')}")

# 8. Phone A sees the like, comment and follower synced back
s, r = call("getFeed", {"viewerId": id_a, "tab": "foryou", "limit": 20})
d = data_of(s, r, "getFeed(A)")
posts = d.get("posts", d if isinstance(d, list) else [])
mine = [p for p in posts if p.get("id") == post_id]
like_count = mine[0].get("likeCount") if mine else None
check("A sees B's like on the post", like_count == 1, f"likeCount={like_count}")
s, r = call("listComments", {"postId": post_id})
d = data_of(s, r, "listComments(A)")
comments = d.get("comments", d if isinstance(d, list) else [])
check("A sees B's comment", any("Hello from phone B" in str(c.get("text", "")) for c in comments),
      f"comments={len(comments)}")
s, r = call("listFollowList", {"accountId": id_a, "kind": "followers"})
d = data_of(s, r, "listFollowList(A)")
followers = d.get("followers", d.get("accounts", d if isinstance(d, list) else []))
check("A sees B as a follower", any(f.get("id") == id_b or f.get("accountId") == id_b
                                    for f in followers), f"followers={len(followers)}")

# 9. Admin still works from the shared data
s, r = call("loginWithContact", {"identifier": "clippro"})
d = data_of(s, r, "loginWithContact(clippro)")
check("admin @clippro signs in", d["account"].get("isAdmin") is True,
      f"username={d['account'].get('username')}")
s, r = call("listAccounts", {})
d = data_of(s, r, "listAccounts")
names = [a.get("username") for a in d.get("accounts", [])]
check("both test accounts visible in shared account list",
      "phonea" in names and "phoneb" in names, f"accounts={names[:6]}")

print(f"\nRESULT: {PASS} passed, {FAIL} failed")
sys.exit(0 if FAIL == 0 else 1)
