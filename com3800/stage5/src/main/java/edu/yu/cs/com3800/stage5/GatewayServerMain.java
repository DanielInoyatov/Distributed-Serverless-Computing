package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main class to start a GatewayServer from command line
 * Usage: java GatewayServerMain <httpPort> <peerPort> <peerEpoch> <serverId> <peerAddresses> <numObservers>
 * 
 * Example peerAddresses format: "0:localhost:8010,1:localhost:8020,2:localhost:8030"
 */
public class GatewayServerMain {
    
    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            System.err.println("Usage: GatewayServerMain <httpPort> <peerPort> <peerEpoch> <serverId> <peerAddresses> <numObservers>");
            System.exit(1);
        }
        
        int httpPort = Integer.parseInt(args[0]);
        int peerPort = Integer.parseInt(args[1]);
        long peerEpoch = Long.parseLong(args[2]);
        long serverId = Long.parseLong(args[3]);
        String peerAddressesStr = args[4];
        int numObservers = Integer.parseInt(args[5]);
        
        // Parse peer addresses
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();
        String[] peers = peerAddressesStr.split(",");
        
        for (String peer : peers) {
            String[] parts = peer.split(":");
            long id = Long.parseLong(parts[0]);
            String host = parts[1];
            int port = Integer.parseInt(parts[2]);
            peerIDtoAddress.put(id, new InetSocketAddress(host, port));
        }
        
        System.out.println("Starting GatewayServer:");
        System.out.println("  HTTP Port: " + httpPort);
        System.out.println("  Peer Port: " + peerPort);
        System.out.println("  Server ID: " + serverId);
        System.out.println("  Epoch: " + peerEpoch);
        System.out.println("  Number of Peers: " + peerIDtoAddress.size());
        
        // Create and start the gateway server
        GatewayServer gatewayServer = new GatewayServer(
            httpPort,
            peerPort,
            peerEpoch,
            serverId,
            peerIDtoAddress,
            numObservers
        );
        
        gatewayServer.start();
        
        // Keep the main thread alive
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("GatewayServer interrupted, shutting down...");
            gatewayServer.shutdown();
        }
    }
}