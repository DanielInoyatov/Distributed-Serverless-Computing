package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main class to start the distributed cluster for demo purposes.
 * This starts only the Gateway server - peer servers are started separately.
 */
public class DemoCluster {
    
    public static void main(String[] args) throws IOException, InterruptedException {
        // Configuration
        int numPeers = 7; // 7 peer servers (IDs 1-7)
        int gatewayHttpPort = 8080;
        int basePeerPort = 8010; // Starting UDP port for peers
        long gatewayID = 0L; // Gateway server ID
        int numberOfObservers = 1; // Just the gateway
        
        // Create peer ID to address mapping for ALL servers (including gateway)
        Map<Long, InetSocketAddress> allPeers = new HashMap<>();
        for (int i = 0; i <= numPeers; i++) {
            long serverID = i;
            int udpPort = basePeerPort + (i * 10);
            allPeers.put(serverID, new InetSocketAddress("localhost", udpPort));
        }
        
        // For the gateway, create a map that EXCLUDES itself (matching test behavior)
        Map<Long, InetSocketAddress> peersForGateway = new HashMap<>(allPeers);
        peersForGateway.remove(gatewayID);
        
        int gatewayUdpPort = allPeers.get(gatewayID).getPort();
        
        System.out.println("Starting GatewayServer:");
        System.out.println("  HTTP Port: " + gatewayHttpPort);
        System.out.println("  Peer Port: " + gatewayUdpPort);
        System.out.println("  Server ID: " + gatewayID);
        System.out.println("  Epoch: 0");
        System.out.println("  Number of Peers: " + allPeers.size());
        
        // Start the gateway server
        GatewayServer gateway = new GatewayServer(
            gatewayHttpPort,
            gatewayUdpPort,
            0L,  // Initial epoch
            gatewayID,
            new ConcurrentHashMap<>(peersForGateway),
            numberOfObservers
        );
        gateway.setDaemon(false); // Keep JVM alive
        gateway.start();
        
        // Small delay to ensure gateway is up
        Thread.sleep(500);
        
        System.out.println("Gateway started on HTTP port: " + gatewayHttpPort);
        System.out.println("Waiting for peer servers to connect...");
        
        // Keep the main thread alive
        while (true) {
            Thread.sleep(1000);
        }
    }
}