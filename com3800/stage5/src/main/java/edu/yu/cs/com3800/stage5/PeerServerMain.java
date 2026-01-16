package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main class to start a PeerServer from command line
 * Usage: java PeerServerMain <serverId> <port> <peerEpoch> <peerAddresses> <gatewayId> <numObservers>
 * 
 * Example peerAddresses format: "0:localhost:8010,1:localhost:8020,2:localhost:8030"
 */
public class PeerServerMain {
    
    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            System.err.println("Usage: PeerServerMain <serverId> <port> <peerEpoch> <peerAddresses> <gatewayId> <numObservers>");
            System.exit(1);
        }
        
        long serverId = Long.parseLong(args[0]);
        int port = Integer.parseInt(args[1]);
        long peerEpoch = Long.parseLong(args[2]);
        String peerAddressesStr = args[3];
        long gatewayId = Long.parseLong(args[4]);
        int numObservers = Integer.parseInt(args[5]);
        
        // Parse peer addresses
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();
        String[] peers = peerAddressesStr.split(",");
        
        for (String peer : peers) {
            String[] parts = peer.split(":");
            long id = Long.parseLong(parts[0]);
            String host = parts[1];
            int peerPort = Integer.parseInt(parts[2]);
            peerIDtoAddress.put(id, new InetSocketAddress(host, peerPort));
        }
        
        System.out.println("Starting PeerServer:");
        System.out.println("  Server ID: " + serverId);
        System.out.println("  Port: " + port);
        System.out.println("  Epoch: " + peerEpoch);
        System.out.println("  Gateway ID: " + gatewayId);
        System.out.println("  Number of Peers: " + peerIDtoAddress.size());
        
        // Create and start the peer server
        PeerServerImpl peerServer = new PeerServerImpl(
            port, 
            peerEpoch, 
            serverId, 
            peerIDtoAddress, 
            gatewayId, 
            numObservers
        );
        
        peerServer.start();
        
        // Keep the main thread alive
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("PeerServer " + serverId + " interrupted, shutting down...");
            peerServer.shutdown();
        }
    }
}