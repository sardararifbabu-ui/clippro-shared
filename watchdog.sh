#!/bin/bash
# ClipPro shared-backend watchdog.
# If GET /clippro-shared stops answering, restart the backend.
# Intended to run every 5 minutes from cron.
set -u
DIR="$HOME/workspace/clippro-server"
LOG="$DIR/watchdog.log"
JAVA="$HOME/workspace/android-build/jdk17/bin/java"
ASSETS="$HOME/workspace/android-build/ClipPro/app/src/main/assets"

if curl -fsS -m 5 "http://127.0.0.1:8080/clippro-shared" >/dev/null 2>&1; then
    exit 0
fi

echo "$(date -Is) backend down - restarting" >> "$LOG"
cd "$DIR" || exit 1
nohup "$JAVA" -cp classes com.clippro.app.SharedMain 8080 data "$ASSETS" >> backend.log 2>&1 &
sleep 3
if curl -fsS -m 5 "http://127.0.0.1:8080/clippro-shared" >/dev/null 2>&1; then
    echo "$(date -Is) backend restarted OK" >> "$LOG"
else
    echo "$(date -Is) backend restart FAILED" >> "$LOG"
    exit 1
fi
