package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import static org.junit.jupiter.api.Assertions.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Stage4Test {

    private static class Cluster {
        final GatewayServer gateway;
        final Map<Long, PeerServerImpl> peerServers;
        final Map<Long, Integer> serverIdToUdpPort;
        final long gatewayId;
        final Set<Long> observerIds;

        Cluster(GatewayServer gateway, Map<Long, PeerServerImpl> peerServers,
                Map<Long, Integer> serverIdToUdpPort, long gatewayId, Set<Long> observerIds) {
            this.gateway = gateway;
            this.peerServers = peerServers;
            this.serverIdToUdpPort = serverIdToUdpPort;
            this.gatewayId = gatewayId;
            this.observerIds = observerIds;
        }

        void shutdown() {
            System.out.println("Shutting down cluster...");
            try {
                if (gateway != null && gateway.getPeerServer() != null) {
                    gateway.getPeerServer().shutdown();
                }
            } catch (Exception e) {
                System.err.println("Error shutting down gateway peer: " + e.getMessage());
            }
            for (PeerServerImpl server : peerServers.values()) {
                try {
                    if (server != null && server != gateway.getPeerServer()) {
                        server.shutdown();
                    }
                } catch (Exception e) {
                    System.err.println("Error shutting down server " + server.getServerId() + ": " + e.getMessage());
                }
            }

            try {
                if (gateway != null) {
                    gateway.shutdown();
                }
            } catch (Exception e) {
                System.err.println("Error shutting down gateway: " + e.getMessage());
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        long getLeaderId() {
            Vote leader = null;
            // Get leader from any voting server
            for (PeerServerImpl server : peerServers.values()) {
                leader = server.getCurrentLeader();
                break;
            }
            return leader != null ? leader.getProposedLeaderID() : -1L;
        }

        long getExpectedLeaderId() {//returns the largest id in the cluster which should correspond with the leader id
            long maxId = -1;
            for (Long id : peerServers.keySet()) {
                if (!observerIds.contains(id) && id != gatewayId) {
                    maxId = Math.max(maxId, id);
                }
            }
            return maxId;
        }

        int countVotingServers() {
            int count = 0;
            for (Long id : peerServers.keySet()) {
                if (!observerIds.contains(id) && id != gatewayId) {
                    count++;
                }
            }
            return count;
        }
    }

    @Test
    @DisplayName("All servers agree on the same leader")
    void testLeaderConsensus() throws IOException, InterruptedException {
        ArrayList<Long> allIds = new ArrayList<>();
        for(long i=1; i<=10;i++){
            allIds.add(i);
        }
        HashSet<Long> allObserverIds = new HashSet<>();
        long ranObserver1 = 2L;
        long ranObserver2 = 7L;
        long ranObserver3 = 3L;
        allObserverIds.add(ranObserver1);
        allObserverIds.add(ranObserver2);
        allObserverIds.add(ranObserver3);
        Cluster cluster = createCluster(allIds, 10L, allObserverIds);

        long gatewayLeader = cluster.gateway.getPeerServer().getCurrentLeader().getProposedLeaderID();
        Map<Long,PeerServerImpl> servers = cluster.peerServers;
        for(Long id : servers.keySet()) {
            assertEquals(gatewayLeader, servers.get(id).getCurrentLeader().getProposedLeaderID());
        }
        assertTrue(gatewayLeader!= cluster.gatewayId);
        cluster.shutdown();
    }

    @Test
    @DisplayName("Cache Test")
    void cacheTest() throws IOException, InterruptedException {
        ArrayList<Long> allIds = new ArrayList<>();
        for(long i=1; i<=10;i++){
            allIds.add(i);
        }
        HashSet<Long> allObserverIds = new HashSet<>();
        long ranObserver1 = 2L;
        long ranObserver2 = 7L;
        long ranObserver3 = 3L;
        allObserverIds.add(ranObserver1);
        allObserverIds.add(ranObserver2);
        allObserverIds.add(ranObserver3);
        Cluster cluster = createCluster(allIds, 10L, allObserverIds);
        String javaCode = """
                public class HelloWorld {
                    public String run() {
                        return "Hello from worker!";
                    }
                }
                """;
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals("Hello from worker!", response.body());
        assertEquals("false" , response.headers().firstValue("Cached-Response").get());

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals("Hello from worker!", response.body());
        assertEquals("true" , response.headers().firstValue("Cached-Response").get());
        cluster.shutdown();
    }

    @Test
    @DisplayName("Testing to make sure RoundRobin Distributes correctly, will check the logs")
    void correctRoundRobinDistributes() throws IOException, InterruptedException {
        ArrayList<Long> allIds = new ArrayList<>();
        for(long i=1; i<=10;i++){
            allIds.add(i);
        }
        HashSet<Long> allObserverIds = new HashSet<>();
        long ranObserver1 = 2L;
        long ranObserver2 = 7L;
        long ranObserver3 = 3L;
        allObserverIds.add(ranObserver1);
        allObserverIds.add(ranObserver2);
        allObserverIds.add(ranObserver3);

        Cluster cluster = createCluster(allIds, 10L, allObserverIds);
        HttpClient client = HttpClient.newHttpClient();

        for (int i = 1; i <= 20; i++) {

            String javaCode = String.format("""
            public class HelloWorld {
                public String run() {
                    return "Hello from worker #%d!";
                }
            }
            """, i);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8888/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                    .build();

            int finalI = i;
            int finalI1 = i;
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response ->
                            System.out.println("Response " + finalI + ": " + response.body())
                    )
                    .exceptionally(ex -> {
                        System.err.println("Request " + finalI1 + " failed: " + ex);
                        return null;
                    });
        }

        Thread.sleep(5000);
        cluster.shutdown();

    }
    @Test
    @DisplayName("Empty request body returns 400 error")
    void testEmptyRequestBody() throws Exception{
        ArrayList<Long> allIds = new ArrayList<>();
        for(long i=1; i<=10;i++){
            allIds.add(i);
        }
        HashSet<Long> allObserverIds = new HashSet<>();
        long ranObserver1 = 2L;
        long ranObserver2 = 7L;
        long ranObserver3 = 3L;
        allObserverIds.add(ranObserver1);
        allObserverIds.add(ranObserver2);
        allObserverIds.add(ranObserver3);

        Cluster cluster = createCluster(allIds, 10L, allObserverIds);

        String javaCode = "";

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertEquals("false" , response.headers().firstValue("Cached-Response").get());
        assertTrue(response.body().contains("Exception"));
        javaCode = """
                public class HelloWorld {
                    public String run() {
                        return "Hello from worker!";
                    }
                }
                """;
        request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals("Hello from worker!", response.body());
        assertEquals("false" , response.headers().firstValue("Cached-Response").get());

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals("Hello from worker!", response.body());
        assertEquals("true" , response.headers().firstValue("Cached-Response").get());
        cluster.shutdown();
    }
    private static Cluster createCluster(List<Long> allIds, long gatewayId, Set<Long> additionalObserverIds)
            throws IOException, InterruptedException {
        long peerEpoch = 0L;
        int numberOfObservers = 1 + additionalObserverIds.size(); // gateway + additional

        // Assign UDP ports
        int baseUdpPort = 8010;
        Map<Long, Integer> idToUdpPort = new HashMap<>();
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();

        for (long id : allIds) {
            int udpPort = baseUdpPort + ((int) id - 1) * 10;
            idToUdpPort.put(id, udpPort);
            peerIDtoAddress.put(id, new InetSocketAddress("localhost", udpPort));
        }

        // Create gateway
        int gatewayUdpPort = idToUdpPort.get(gatewayId);
        Map<Long, InetSocketAddress> peersForGateway = new HashMap<>(peerIDtoAddress);
        peersForGateway.remove(gatewayId);

        GatewayServer gateway = new GatewayServer(
                8888,
                gatewayUdpPort,
                peerEpoch,
                gatewayId,
                new ConcurrentHashMap<>(peersForGateway),
                numberOfObservers
        );

        // Create all non-gateway servers
        Map<Long, PeerServerImpl> servers = new HashMap<>();
        for (long id : allIds) {
            if (id == gatewayId) {
                continue;
            }

            int udpPort = idToUdpPort.get(id);
            Map<Long, InetSocketAddress> peersMinusSelf = new HashMap<>(peerIDtoAddress);
            peersMinusSelf.remove(id);

            PeerServerImpl server = new PeerServerImpl(
                    udpPort,
                    peerEpoch,
                    id,
                    peersMinusSelf,
                    gatewayId,
                    numberOfObservers
            );

            if (additionalObserverIds.contains(id)) {
                server.setPeerState(PeerServer.ServerState.OBSERVER);
            }
            servers.put(id, server);
            Thread.sleep(50);
        }
        gateway.start();
        for(PeerServerImpl server : servers.values()) {
            server.start();
        }
        Thread.sleep(4000);
        Set<Long> allObserverIds = new HashSet<>(additionalObserverIds);
        allObserverIds.add(gatewayId);

        return new Stage4Test.Cluster(gateway, servers, idToUdpPort, gatewayId, allObserverIds);
    }
    @AfterEach
    void waitForPortRelease() {
        long endTime = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(Math.max(100, endTime - System.currentTimeMillis()));
            } catch (InterruptedException e) {
                // Ignore interrupts during cleanup
            }
        }
    }
}
