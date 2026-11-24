package com.clippro.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Standalone launcher for the ClipPro shared backend.
 *
 * Runs the exact same ClipProServer code the phone APK uses, but on this
 * machine with a fixed port and a shared data directory, so every phone
 * that points at it reads and writes ONE store (accounts, posts, likes,
 * comments, follows, messages, settings) instead of each phone keeping
 * its own copy.
 *
 * Usage: java -cp classes com.clippro.app.SharedMain [port] [dataDir] [assetsDir]
 *   port     - loopback port to listen on (default 8080)
 *   dataDir  - directory holding the clippro/ JSON store + media (default ./data)
 *   assetsDir - directory with starter/ media + www/ client (default ./assets)
 *
 * The server seeds the @clippro admin account and the six starter posts on
 * first start, exactly like the phone app does.
 */
public final class SharedMain {

    public static void main(String[] args) throws Exception {
        int port = 8080;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            // Render (and most cloud hosts) assign the port via the PORT
            // env var. An explicit command-line arg still wins.
            String portEnv = System.getenv("PORT");
            if (portEnv != null && !portEnv.isEmpty()) {
                port = Integer.parseInt(portEnv);
            }
        }
        File dataDir = new File(args.length > 1 ? args[1] : "data");
        File assetsDir = new File(args.length > 2 ? args[2] : "assets");

        ClipProServer server = new ClipProServer(path -> {
            try {
                File f = new File(assetsDir, path);
                if (!f.isFile()) {
                    return null;
                }
                return Files.readAllBytes(f.toPath());
            } catch (IOException e) {
                return null;
            }
        }, dataDir);
        server.setSharedBackend(true);
        server.start(port);
        String bindHost = System.getenv("BIND_HOST");
        if (bindHost == null || bindHost.isEmpty()) {
            bindHost = "127.0.0.1";
        }
        System.out.println("ClipPro shared backend listening on " + bindHost + ":" + server.getPort());
        System.out.println("Data dir: " + dataDir.getAbsolutePath());
        System.out.println("Mode: " + server.getMode());
        System.out.flush();
        Object lock = new Object();
        synchronized (lock) {
            lock.wait();
        }
    }
}
