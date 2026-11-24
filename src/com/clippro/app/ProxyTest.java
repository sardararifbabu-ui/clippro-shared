package com.clippro.app;

import java.io.File;

/**
 * Test harness: runs a ClipProServer exactly like the APK's loopback server
 * (own data dir), optionally pointed at a shared backend via setRemoteBase.
 * Used to prove the remote-first proxy path and the local fallback path.
 *
 * Usage: java -cp classes com.clippro.app.ProxyTest <port> <dataDir> [remoteBase]
 */
public final class ProxyTest {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        File dataDir = new File(args[1]);
        String remote = args.length > 2 ? args[2] : null;

        ClipProServer server = new ClipProServer(path -> null, dataDir);
        if (remote != null && !remote.isEmpty()) {
            server.setRemoteBase(remote);
        }
        server.start(port);
        boolean ok = remote != null && !remote.isEmpty() && server.probeRemote();
        server.setRemoteEnabled(ok);
        System.out.println("proxy on 127.0.0.1:" + server.getPort()
                + " remote=" + remote + " probe=" + ok + " mode=" + server.getMode());
        System.out.flush();
        Object lock = new Object();
        synchronized (lock) {
            lock.wait();
        }
    }
}
