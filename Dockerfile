# ClipPro shared backend — Dockerfile for Render (free tier)
#
# Build:   docker build -t clippro-shared .
# Run:     docker run -e PORT=8080 -e BIND_HOST=0.0.0.0 -p 8080:8080 clippro-shared
# Health:  GET /clippro-shared  -> {"mode":"shared",...}
#
# Plain Java 17, no frameworks, no build tool. The server is a single
# ClipProServer class plus a tiny JSON helper and the SharedMain launcher.
# Data lives in ./data as JSON files (see the free-tier warning in
# RENDER-DEPLOY.md: Render free disk is ephemeral — data is lost when the
# service sleeps or redeploys).

# ---- build stage: compile the server -------------------------------------
FROM eclipse-temurin:17-jdk AS build
WORKDIR /app
COPY src/ ./src/
RUN mkdir -p classes \
    && javac -d classes src/com/clippro/app/ClipProServer.java \
                      src/com/clippro/app/Json.java \
                      src/com/clippro/app/SharedMain.java \
                      src/com/clippro/app/ProxyTest.java

# ---- runtime stage: JRE + compiled classes + starter assets --------------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/classes ./classes
COPY assets/ ./assets/
# The server creates ./data on first start and seeds the @clippro admin
# account plus the six starter posts (exactly like the phone app does).
# NOTE: ./data is ephemeral on Render free tier — see RENDER-DEPLOY.md.
RUN mkdir -p data
EXPOSE 8080
# PORT is set by Render automatically; BIND_HOST=0.0.0.0 is set in
# render.yaml so Render's proxy can reach the server. SharedMain reads
# PORT from the environment when no port argument is given.
CMD ["java", "-cp", "classes", "com.clippro.app.SharedMain"]
