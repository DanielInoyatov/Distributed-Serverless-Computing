package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Main class to start an individual peer server for demo purposes.
 * Each peer server runs in its own JVM.
 */
public class DemoPeerServer {
    
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java DemoPeerServer <serverID>");
            System.exit(1);
        }
        
        long serverID = Long.parseLong(args[0]);
        int basePeerPort = 8010;
        long gatewayID = 0L;
        int numberOfObservers = 1;
        int numPeers = 7;
        
        // Create peer ID to address mapping for ALL servers
        Map<Long, InetSocketAddress> allPeers = new HashMap<>();
        for (int i = 0; i <= numPeers; i++) {
            int udpPort = basePeerPort + (i * 10);
            allPeers.put((long) i, new InetSocketAddress("localhost", udpPort));
        }
        
        // For this server, create a map that EXCLUDES itself (matching test behavior)
        Map<Long, InetSocketAddress> peersMinusSelf = new HashMap<>(allPeers);
        peersMinusSelf.remove(serverID);
        
        // Get this server's port
        int udpPort = allPeers.get(serverID).getPort();
        
        System.out.println("Starting PeerServer:");
        System.out.println("  Server ID: " + serverID);
        System.out.println("  Port: " + udpPort);
        System.out.println("  Epoch: 0");
        System.out.println("  Gateway ID: " + gatewayID);
        System.out.println("  Number of Peers: " + allPeers.size());
        
        // Create and start peer server
        PeerServerImpl peerServer = new PeerServerImpl(
            udpPort,
            0L, // Initial epoch
            serverID,
            peersMinusSelf,
            gatewayID,
            numberOfObservers
        );
        
        peerServer.start();
        
        System.out.println("Peer Server " + serverID + " is running.");
        
        // Keep the JVM alive
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("Peer Server " + serverID + " shutting down.");
        }
    }
}