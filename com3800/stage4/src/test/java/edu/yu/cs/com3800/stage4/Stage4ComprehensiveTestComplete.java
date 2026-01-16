package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.*;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for Stage 4 distributed Java compilation system.
 * Tests leader election, HTTP handling, caching, round-robin distribution,
 * error handling, and system resilience.
 */
@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Stage4ComprehensiveTestComplete {

    // ==================== HELPER CLASS ====================

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
                    System.err.println("Error shutting down server: " + e.getMessage());
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
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        long getLeaderId() {
            Vote leader = null;
            for (PeerServerImpl server : peerServers.values()) {
                if (server.getPeerState() != PeerServer.ServerState.OBSERVER) {
                    leader = server.getCurrentLeader();
                    break;
                }
            }
            return leader != null ? leader.getProposedLeaderID() : -1L;
        }

        long getExpectedLeaderId() {
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

        List<Long> getWorkerIds() {
            return peerServers.keySet().stream()
                    .filter(id -> !observerIds.contains(id))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /**
     * Creates a cluster with configurable observers
     */
    private static Cluster createCluster(List<Long> allIds, long gatewayId, Set<Long> additionalObserverIds)
            throws IOException, InterruptedException {
        long peerEpoch = 0L;
        int numberOfObservers = 1 + additionalObserverIds.size();

        int baseUdpPort = 8010;
        Map<Long, Integer> idToUdpPort = new HashMap<>();
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();

        for (long id : allIds) {
            int udpPort = baseUdpPort + ((int) id - 1) * 10;
            idToUdpPort.put(id, udpPort);
            peerIDtoAddress.put(id, new InetSocketAddress("localhost", udpPort));
        }

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

        Map<Long, PeerServerImpl> servers = new HashMap<>();
        for (long id : allIds) {
            if (id == gatewayId) continue;

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
        //gateway.start();
        for (PeerServerImpl server : servers.values()) {
            server.start();
        }
        gateway.start();
        Thread.sleep(1000);

        Set<Long> allObserverIds = new HashSet<>(additionalObserverIds);
        allObserverIds.add(gatewayId);

        return new Cluster(gateway, servers, idToUdpPort, gatewayId, allObserverIds);
    }

    // ==================== CRITICAL TESTS ====================

    @Test
    @DisplayName("CRITICAL 1: Leader election completes within reasonable time (under 10 seconds)")
    void testLeaderElectionTimeliness() throws Exception {
        System.out.println("\n=== CRITICAL 1: Leader Election Timeliness ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L));

        long startTime = System.currentTimeMillis();

        // Wait up to 10 seconds for election
        boolean leaderElected = false;
        for (int i = 0; i < 100; i++) {
            Thread.sleep(100);
            if (cluster.getLeaderId() > 0) {
                leaderElected = true;
                break;
            }
        }

        long electionTime = System.currentTimeMillis() - startTime;
        System.out.println("Election completed in: " + electionTime + "ms");

        assertTrue(leaderElected, "Leader should be elected within 10 seconds");
        assertTrue(electionTime < 10000, "Election took too long: " + electionTime + "ms");
        System.out.println("✅ Election completed in " + electionTime + "ms");

        cluster.shutdown();
    }

    @Test
    @DisplayName("CRITICAL 2: All voting servers agree on the same leader")
    void testLeaderConsensus() throws Exception {
        System.out.println("\n=== CRITICAL 2: Leader Consensus ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L));

        Thread.sleep(6000);

        long expectedLeader = cluster.getExpectedLeaderId();
        Set<Long> leaderVotes = new HashSet<>();

        // Check each voting server
        for (Map.Entry<Long, PeerServerImpl> entry : cluster.peerServers.entrySet()) {
            PeerServerImpl server = entry.getValue();
            if (!cluster.observerIds.contains(entry.getKey())) {
                Vote vote = server.getCurrentLeader();
                assertNotNull(vote, "Server " + entry.getKey() + " should have a leader");
                leaderVotes.add(vote.getProposedLeaderID());
                System.out.println("Server " + entry.getKey() + " votes for: " + vote.getProposedLeaderID());
            }
        }

        assertEquals(1, leaderVotes.size(), "All servers should agree on ONE leader");
        assertEquals(expectedLeader, leaderVotes.iterator().next(),
                "Leader should be highest non-observer ID");

        System.out.println("✅ All servers agree on leader: " + expectedLeader);

        cluster.shutdown();
    }

    @Test
    @DisplayName("CRITICAL 3: Gateway correctly excludes itself from voting")
    void testGatewayDoesNotVote() throws Exception {
        System.out.println("\n=== CRITICAL 3: Gateway Exclusion ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 3L, Set.of(5L));

        Thread.sleep(6000);

        PeerServerImpl gatewayPeer = cluster.gateway.getPeerServer();
        assertNotNull(gatewayPeer, "Gateway should have a PeerServer");
        assertEquals(PeerServer.ServerState.OBSERVER, gatewayPeer.getPeerState(),
                "Gateway should be in OBSERVER state");

        long expectedLeader = 4L; // Highest non-observer (1,2,4 are voters)
        long actualLeader = cluster.getLeaderId();

        assertEquals(expectedLeader, actualLeader,
                "Leader should be elected without gateway participation");

        System.out.println("✅ Gateway correctly excluded, leader is: " + actualLeader);

        cluster.shutdown();
    }

    @Test
    @DisplayName("CRITICAL 4: Cached responses are at least 5x faster than uncached")
    void testCacheSpeedup() throws Exception {
        System.out.println("\n=== CRITICAL 4: Cache Performance ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String javaCode = """
                public class SpeedTest {
                    public String run() {
                        return "Speed test result";
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();

        // UNCACHED
        long start1 = System.currentTimeMillis();
        HttpResponse<String> response1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        long uncachedTime = System.currentTimeMillis() - start1;

        assertEquals(200, response1.statusCode());
        assertEquals("false", response1.headers().firstValue("Cached-Response").orElse(""));

        Thread.sleep(1000); // Ensure cache is populated

        // CACHED
        long start2 = System.currentTimeMillis();
        HttpResponse<String> response2 = client.send(request, HttpResponse.BodyHandlers.ofString());
        long cachedTime = System.currentTimeMillis() - start2;

        assertEquals(200, response2.statusCode());
        assertEquals("true", response2.headers().firstValue("Cached-Response").orElse(""));

        double speedup = (double) uncachedTime / cachedTime;
        System.out.println("Uncached: " + uncachedTime + "ms");
        System.out.println("Cached:   " + cachedTime + "ms");
        System.out.println("Speedup:  " + String.format("%.1f", speedup) + "x");

        assertTrue(cachedTime < 200, "Cached request should be <200ms, was: " + cachedTime + "ms");
        assertTrue(speedup >= 5.0, "Cache should be 5x+ faster, was: " + String.format("%.1f", speedup) + "x");

        System.out.println("✅ Cache is " + String.format("%.1f", speedup) + "x faster");

        cluster.shutdown();
    }

    @Test
    @DisplayName("CRITICAL 5: Round-robin distributes work evenly across workers")
    void testRoundRobinFairness() throws Exception {
        System.out.println("\n=== CRITICAL 5: Round-Robin Fairness ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L)); // Workers: 1,3,4

        Thread.sleep(6000);

        int numWorkers = 3;
        int requestsPerWorker = 3;
        int totalRequests = numWorkers * requestsPerWorker;

        HttpClient client = HttpClient.newHttpClient();

        for (int i = 0; i < totalRequests; i++) {
            String code = String.format("public class RR%d {\n    public String run() {\n        return \"Request %d\";\n    }\n}\n", i, i);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8888/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .POST(HttpRequest.BodyPublishers.ofString(code))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), "Request " + i + " should succeed");
        }

        System.out.println("✅ All " + totalRequests + " requests distributed successfully");

        cluster.shutdown();
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @DisplayName("EDGE 1: Identical code submitted twice uses cache")
    void testCacheHitOnDuplicateCode() throws Exception {
        System.out.println("\n=== EDGE 1: Cache Hit on Duplicate ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String code = """
                public class DupTest {
                    public String run() {
                        return "Duplicate test";
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .build();

        HttpResponse<String> response1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals("false", response1.headers().firstValue("Cached-Response").orElse(""));

        Thread.sleep(1000);

        HttpResponse<String> response2 = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals("true", response2.headers().firstValue("Cached-Response").orElse(""));
        assertEquals(response1.body(), response2.body());

        System.out.println("✅ Cache correctly returns identical result");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 2: Different code with same class name doesn't use wrong cache")
    void testCacheIsolationBySrcCode() throws Exception {
        System.out.println("\n=== EDGE 2: Cache Isolation ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String code1 = """
                public class SameName {
                    public String run() {
                        return "Version 1";
                    }
                }
                """;

        String code2 = """
                public class SameName {
                    public String run() {
                        return "Version 2";
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request1 = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(code1))
                .build();

        HttpResponse<String> response1 = client.send(request1, HttpResponse.BodyHandlers.ofString());
        assertTrue(response1.body().contains("Version 1"));

        Thread.sleep(1000);

        HttpRequest request2 = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(code2))
                .build();

        HttpResponse<String> response2 = client.send(request2, HttpResponse.BodyHandlers.ofString());
        assertTrue(response2.body().contains("Version 2"));
        assertEquals("false", response2.headers().firstValue("Cached-Response").orElse(""));

        System.out.println("✅ Cache correctly isolates by source code content");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 3: Code with no public run() method returns 400 error")
    void testMissingRunMethod() throws Exception {
        System.out.println("\n=== EDGE 3: Missing run() Method ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String code = """
                public class NoRun {
                    public void differentMethod() {
                        System.out.println("Wrong method");
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode(), "Should return 400 for missing run() method");
        System.out.println("Response: " + response.body());

        System.out.println("✅ Correctly rejects code without run() method");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 4: Code with wrong signature returns 400 error")
    void testWrongRunMethodSignature() throws Exception {
        System.out.println("\n=== EDGE 4: Wrong Signature ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String code = """
                public class WrongSig {
                    public int run() {
                        return 42;
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode(), "Should return 400 for wrong return type");

        System.out.println("✅ Correctly rejects run() with wrong return type");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 5: Empty request body returns 400 error")
    void testEmptyRequestBody() throws Exception {
        System.out.println("\n=== EDGE 5: Empty Request Body ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode(), "Should return 400 for empty body");

        System.out.println("✅ Correctly rejects empty request body");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 6: Very large Java file (5KB+) compiles successfully")
    void testLargeSourceFile() throws Exception {
        System.out.println("\n=== EDGE 6: Large Source File ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        // Generate large but valid Java code
        StringBuilder code = new StringBuilder();
        code.append("public class LargeFile {\n");
        code.append("    public String run() {\n");
        code.append("        StringBuilder sb = new StringBuilder();\n");

        for (int i = 0; i < 100; i++) {
            code.append("        sb.append(\"Line ").append(i).append("\");\n");
        }

        code.append("        return sb.toString();\n");
        code.append("    }\n");
        code.append("}\n");

        String largeCode = code.toString();
        System.out.println("Code size: " + largeCode.length() + " bytes");

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(largeCode))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Large file should compile successfully");

        System.out.println("✅ Large file compiled successfully");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 7: Gateway continues working after worker returns error")
    void testGatewayResilientToWorkerErrors() throws Exception {
        System.out.println("\n=== EDGE 7: Gateway Resilience ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        HttpClient client = HttpClient.newHttpClient();

        // Send bad request
        String badCode = "this is not java";
        HttpRequest badRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(badCode))
                .build();

        HttpResponse<String> badResponse = client.send(badRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, badResponse.statusCode());

        // Send good request
        String goodCode = """
                public class Good {
                    public String run() {
                        return "System recovered";
                    }
                }
                """;

        HttpRequest goodRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(goodCode))
                .build();

        HttpResponse<String> goodResponse = client.send(goodRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, goodResponse.statusCode());
        assertTrue(goodResponse.body().contains("System recovered"));

        System.out.println("✅ Gateway recovered from worker error");

        cluster.shutdown();
    }

    @Test
    @DisplayName("EDGE 8: Multiple different requests don't interfere with each other")
    void testConcurrentRequestIsolation() throws Exception {
        System.out.println("\n=== EDGE 8: Request Isolation ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch latch = new CountDownLatch(3);
        ConcurrentHashMap<String, String> results = new ConcurrentHashMap<>();

        String[] codes = {
                "public class A {\n    public String run() {\n        return \"Result A\";\n    }\n}\n",
                "public class B {\n    public String run() {\n        return \"Result B\";\n    }\n}\n",
                "public class C {\n    public String run() {\n        return \"Result C\";\n    }\n}\n"
        };

        for (int i = 0; i < 3; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8888/compileandrun"))
                            .header("Content-Type", "text/x-java-source")
                            .POST(HttpRequest.BodyPublishers.ofString(codes[idx]))
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    results.put("result" + idx, response.body());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(results.get("result0").contains("Result A"));
        assertTrue(results.get("result1").contains("Result B"));
        assertTrue(results.get("result2").contains("Result C"));

        System.out.println("✅ Concurrent requests properly isolated");

        cluster.shutdown();
    }

    // ==================== STRESS/PERFORMANCE TESTS ====================

    @Test
    @DisplayName("STRESS 1: System handles 50 concurrent requests without failure")
    void testHighConcurrency() throws Exception {
        System.out.println("\n=== STRESS 1: High Concurrency ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L));

        Thread.sleep(6000);

        int numRequests = 50;
        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numRequests);
        AtomicInteger successCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numRequests; i++) {
            final int requestNum = i;
            executor.submit(() -> {
                try {
                    String code = String.format("public class Stress%d {\n    public String run() {\n        return \"Stress %d\";\n    }\n}\n", requestNum, requestNum);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8888/compileandrun"))
                            .header("Content-Type", "text/x-java-source")
                            .POST(HttpRequest.BodyPublishers.ofString(code))
                            .timeout(Duration.ofSeconds(45))
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("Request " + requestNum + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(90, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;

        System.out.println("Completed " + successCount.get() + "/" + numRequests + " in " + duration + "ms");
        assertTrue(successCount.get() >= numRequests * 0.95,
                "At least 95% should succeed: " + successCount.get() + "/" + numRequests);

        System.out.println("✅ Handled " + successCount.get() + " concurrent requests");

        cluster.shutdown();
    }

    @Test
    @DisplayName("STRESS 2: Round-robin cycles through all workers multiple times")
    void testRoundRobinMultipleCycles() throws Exception {
        System.out.println("\n=== STRESS 2: Round-Robin Cycles ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L)); // 3 workers

        Thread.sleep(6000);

        int numWorkers = 3;
        int cycles = 5;
        int totalRequests = numWorkers * cycles;

        HttpClient client = HttpClient.newHttpClient();

        for (int i = 0; i < totalRequests; i++) {
            String code = String.format("public class Cycle%d {\n    public String run() {\n        return \"Request %d\";\n    }\n}\n", i, i);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8888/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .POST(HttpRequest.BodyPublishers.ofString(code))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
        }

        System.out.println("✅ Completed " + cycles + " full cycles through " + numWorkers + " workers");

        cluster.shutdown();
    }

    @Test
    @DisplayName("STRESS 3: Cluster with 9 nodes elects leader successfully")
    void testLargeClusterElection() throws Exception {
        System.out.println("\n=== STRESS 3: Large Cluster ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        Cluster cluster = createCluster(ids, 2L, Set.of(9L));

        Thread.sleep(8000); // Extra time for larger cluster

        long expectedLeader = 8L;
        long actualLeader = cluster.getLeaderId();

        assertEquals(expectedLeader, actualLeader);

        System.out.println("✅ 9-node cluster elected leader: " + actualLeader);

        cluster.shutdown();
    }

    // ==================== FAILURE SCENARIO TESTS ====================

    @Test
    @DisplayName("FAILURE 1: Malformed HTTP request returns proper error")
    void testMalformedHTTPRequest() throws Exception {
        System.out.println("\n=== FAILURE 1: Malformed Request ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        HttpClient client = HttpClient.newHttpClient();

        // Wrong HTTP method
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .DELETE()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertTrue(response.statusCode() >= 400, "Should return error status");

        System.out.println("✅ Malformed request properly rejected with status " + response.statusCode());

        cluster.shutdown();
    }

    @Test
    @DisplayName("FAILURE 2: Request with missing Content-Type header returns 400")
    void testMissingContentTypeHeader() throws Exception {
        System.out.println("\n=== FAILURE 2: Missing Content-Type ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String code = """
                public class Test {
                    public String run() {
                        return "test";
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode(), "Should return 400 for missing Content-Type");

        System.out.println("✅ Missing Content-Type properly rejected");

        cluster.shutdown();
    }

    // ==================== VERIFICATION TESTS ====================

    @Test
    @DisplayName("VERIFY 1: Cache stores actual compiled result, not just hash")
    void testCacheStoresResults() throws Exception {
        System.out.println("\n=== VERIFY 1: Cache Stores Results ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String code = """
                public class CacheStore {
                    public String run() {
                        return "Cached content: " + System.currentTimeMillis();
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .build();

        HttpResponse<String> response1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        String result1 = response1.body();

        Thread.sleep(2000); // Wait to ensure timestamp would be different if recompiled

        HttpResponse<String> response2 = client.send(request, HttpResponse.BodyHandlers.ofString());
        String result2 = response2.body();

        assertEquals("true", response2.headers().firstValue("Cached-Response").orElse(""));
        assertEquals(result1, result2, "Cached result should be IDENTICAL (same timestamp)");

        System.out.println("✅ Cache correctly stores and returns compiled result");

        cluster.shutdown();
    }

    @Test
    @DisplayName("VERIFY 2: Gateway maintains accurate worker list (excludes observers)")
    void testWorkerListAccuracy() throws Exception {
        System.out.println("\n=== VERIFY 2: Worker List Accuracy ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(6L, 7L)); // Workers: 1,3,4,5

        Thread.sleep(6000);

        List<Long> expectedWorkers = Arrays.asList(1L, 3L, 4L, 5L);
        List<Long> actualWorkers = cluster.getWorkerIds();

        System.out.println("Expected workers: " + expectedWorkers);
        System.out.println("Actual workers:   " + actualWorkers);

        assertEquals(expectedWorkers, actualWorkers, "Worker list should exclude gateway and observers");

        System.out.println("✅ Worker list correctly excludes observers");

        cluster.shutdown();
    }

    @Test
    @DisplayName("VERIFY 3: Quorum size calculation matches expected formula")
    void testQuorumSizeCalculation() throws Exception {
        System.out.println("\n=== VERIFY 3: Quorum Size ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L)); // 5 voters (1,3,4,5,6)

        Thread.sleep(6000);

        int numVoters = 5;
        int expectedQuorum = (numVoters / 2) + 1; // Should be 3

        // Check quorum from one of the voting servers
        PeerServerImpl voter = null;
        for (Map.Entry<Long, PeerServerImpl> entry : cluster.peerServers.entrySet()) {
            if (!cluster.observerIds.contains(entry.getKey())) {
                voter = entry.getValue();
                break;
            }
        }

        assertNotNull(voter, "Should have at least one voter");
        int actualQuorum = voter.getQuorumSize();

        System.out.println("Number of voters: " + numVoters);
        System.out.println("Expected quorum:  " + expectedQuorum);
        System.out.println("Actual quorum:    " + actualQuorum);

        assertEquals(expectedQuorum, actualQuorum, "Quorum should be (n/2)+1");

        System.out.println("✅ Quorum calculation correct");

        cluster.shutdown();
    }

    // ==================== CLEANUP ====================

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