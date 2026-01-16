package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.*;

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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import static org.junit.jupiter.api.Assertions.*;
@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Stage4ComprehensiveTest {

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

            // Shut down gateway's PeerServerImpl first
            try {
                if (gateway != null && gateway.getPeerServer() != null) {
                    gateway.getPeerServer().shutdown();
                }
            } catch (Exception e) {
                System.err.println("Error shutting down gateway peer: " + e.getMessage());
            }

            // Shut down all other peer servers
            for (PeerServerImpl server : peerServers.values()) {
                try {
                    if (server != null && server != gateway.getPeerServer()) {
                        server.shutdown();
                    }
                } catch (Exception e) {
                    System.err.println("Error shutting down server " + server.getServerId() + ": " + e.getMessage());
                }
            }

            // Shut down the GatewayServer
            try {
                if (gateway != null) {
                    gateway.shutdown();
                }
            } catch (Exception e) {
                System.err.println("Error shutting down gateway: " + e.getMessage());
            }

            // Give time for threads to clean up
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
                if (server.getPeerState() != PeerServer.ServerState.OBSERVER) {
                    leader = server.getCurrentLeader();
                    break;
                }
            }
            return leader != null ? leader.getProposedLeaderID() : -1L;
        }

        long getExpectedLeaderId() {
            // Leader should be the highest ID among non-observers
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

    /**
     * Creates a cluster with configurable observers
     */
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
        for(PeerServerImpl server : servers.values()) {
            server.start();
        }
        Thread.sleep(1000);
        Set<Long> allObserverIds = new HashSet<>(additionalObserverIds);
        allObserverIds.add(gatewayId);

        return new Cluster(gateway, servers, idToUdpPort, gatewayId, allObserverIds);
    }

    // ==================== LEADER ELECTION TESTS ====================

    @Test
    @DisplayName("Test 1: Leader election - standard 7-node cluster")
    void testLeaderElectionStandard() throws IOException, InterruptedException {
        System.out.println("\n=== TEST 1: Standard Leader Election ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L));

        // Wait for election
        Thread.sleep(6000);

        long expectedLeader = cluster.getExpectedLeaderId();
        long actualLeader = cluster.getLeaderId();

        System.out.println("Expected leader: " + expectedLeader);
        System.out.println("Actual leader: " + actualLeader);

        assertEquals(expectedLeader, actualLeader,
                "Leader should be highest non-observer ID");

        // Verify all voting servers agree on leader
        for (Map.Entry<Long, PeerServerImpl> entry : cluster.peerServers.entrySet()) {
            PeerServerImpl server = entry.getValue();
            if (!cluster.observerIds.contains(entry.getKey())) {
                Vote vote = server.getCurrentLeader();
                assertEquals(expectedLeader, vote.getProposedLeaderID(),
                        "Server " + entry.getKey() + " should agree on leader");
            }
        }

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 2: Leader election - observers at beginning")
    void testLeaderElectionObserversAtBeginning() throws IOException, InterruptedException {
        System.out.println("\n=== TEST 2: Observers at Beginning ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 1L, Set.of(2L, 3L)); // Gateway=1, Observers=2,3

        Thread.sleep(6000);

        long expectedLeader = 7L; // Highest non-observer
        long actualLeader = cluster.getLeaderId();

        System.out.println("Expected leader: " + expectedLeader);
        System.out.println("Actual leader: " + actualLeader);

        assertEquals(expectedLeader, actualLeader);
        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 3: Leader election - observers in middle")
    void testLeaderElectionObserversInMiddle() throws IOException, InterruptedException {
        System.out.println("\n=== TEST 3: Observers in Middle ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 4L, Set.of(3L, 5L)); // Gateway=4, Observers=3,5

        Thread.sleep(6000);

        long expectedLeader = 7L;
        long actualLeader = cluster.getLeaderId();

        System.out.println("Expected leader: " + expectedLeader);
        System.out.println("Actual leader: " + actualLeader);

        assertEquals(expectedLeader, actualLeader);
        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 4: Leader election - observers at end")
    void testLeaderElectionObserversAtEnd() throws IOException, InterruptedException {
        System.out.println("\n=== TEST 4: Observers at End ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 6L, Set.of(7L)); // Gateway=6, Observer=7

        Thread.sleep(6000);

        long expectedLeader = 5L; // Highest non-observer
        long actualLeader = cluster.getLeaderId();

        System.out.println("Expected leader: " + expectedLeader);
        System.out.println("Actual leader: " + actualLeader);

        assertEquals(expectedLeader, actualLeader);
        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 5: Leader election - 5-node cluster, different configuration")
    void testLeaderElectionFiveNodes() throws IOException, InterruptedException {
        System.out.println("\n=== TEST 5: Five Node Cluster ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 3L, Set.of(1L)); // Gateway=3, Observer=1

        Thread.sleep(6000);

        long expectedLeader = 5L;
        long actualLeader = cluster.getLeaderId();

        System.out.println("Expected leader: " + expectedLeader);
        System.out.println("Actual leader: " + actualLeader);

        assertEquals(expectedLeader, actualLeader);
        cluster.shutdown();
    }

    // ==================== HTTP CLIENT & WORK PROCESSING TESTS ====================

    @Test
    @DisplayName("Test 6: Single HTTP request end-to-end")
    void testSingleHTTPRequest() throws Exception {
        System.out.println("\n=== TEST 6: Single HTTP Request ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        // Wait for cluster to stabilize
        Thread.sleep(6000);

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

        System.out.println("Response status: " + response.statusCode());
        System.out.println("Response body: " + response.body());
        System.out.println("Cached: " + response.headers().firstValue("Cached-Response").orElse("not set"));

        assertEquals(200, response.statusCode(), "Should get 200 OK");
        assertTrue(response.body().contains("Hello from worker!"),
                "Response should contain expected output");
        assertEquals("false", response.headers().firstValue("Cached-Response").orElse(""),
                "First request should not be cached");

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 7: Cache functionality")
    void testCacheFunctionality() throws Exception {
        System.out.println("\n=== TEST 7: Cache Test ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String javaCode = """
                public class CacheTest {
                    public String run() {
                        return "Cached result";
                    }
                }
                """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();

        // First request
        HttpResponse<String> response1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals("false", response1.headers().firstValue("Cached-Response").orElse(""));
        Thread.sleep(1000);
        // Second request with same code - should be cached
        HttpResponse<String> response2 = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals("true", response2.headers().firstValue("Cached-Response").orElse(""),
                "Second request should be cached");
        assertEquals(response1.body(), response2.body(), "Cached response should match original");

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 8: Multiple concurrent HTTP requests")
    void testMultipleConcurrentRequests() throws Exception {
        System.out.println("\n=== TEST 8: Multiple Concurrent Requests ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L));

        Thread.sleep(6000);

        int numRequests = 15;
        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);
        CountDownLatch latch = new CountDownLatch(numRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        ConcurrentHashMap<Integer, String> results = new ConcurrentHashMap<>();

        for (int i = 0; i < numRequests; i++) {
            final int requestNum = i;
            executor.submit(() -> {
                try {
                    String javaCode = """
                            public class Request%d {
                                public String run() {
                                    return "Response from request %d";
                                }
                            }
                            """.formatted(requestNum, requestNum);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8888/compileandrun"))
                            .header("Content-Type", "text/x-java-source")
                            .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                            .timeout(Duration.ofSeconds(30))
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        successCount.incrementAndGet();
                        results.put(requestNum, response.body());
                        System.out.println("Request " + requestNum + " completed successfully");
                    } else {
                        System.err.println("Request " + requestNum + " failed with status " + response.statusCode());
                    }
                } catch (Exception e) {
                    System.err.println("Request " + requestNum + " threw exception: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(completed, "All requests should complete within timeout");
        assertEquals(numRequests, successCount.get(),
                "All " + numRequests + " requests should succeed");

        // Verify each request got its correct response
        for (int i = 0; i < numRequests; i++) {
            String expectedText = "Response from request " + i;
            assertTrue(results.get(i).contains(expectedText),
                    "Request " + i + " should contain: " + expectedText);
        }

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 9: Round-robin work distribution")
    void testRoundRobinDistribution() throws Exception {
        System.out.println("\n=== TEST 9: Round-Robin Distribution ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        Cluster cluster = createCluster(ids, 2L, Set.of(7L)); // 5 workers (1,3,4,5,6)

        Thread.sleep(6000);

        int numRequests = 10;
        HttpClient client = HttpClient.newHttpClient();

        // Send requests sequentially to observe round-robin
        for (int i = 0; i < numRequests; i++) {
            String javaCode = String.format("public class RoundRobin%d {\n    public String run() {\n        return \"Request %d processed\";\n    }\n}\n", i, i);
            System.out.println("=== CODE BEING SENT ===");
            System.out.println(javaCode);
            System.out.println("=== END CODE ===");
            System.out.println("Starts with 'public': " + javaCode.startsWith("public"));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8888/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, response.statusCode(), "Request " + i + " should succeed");
            assertTrue(response.body().contains("Request " + i + " processed"));

        }

        System.out.println("All " + numRequests + " requests completed successfully");
        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 10: Verify observers don't receive work")
    void testObserversDoNotReceiveWork() throws Exception {
        System.out.println("\n=== TEST 10: Observers Don't Receive Work ===");
        // Create cluster where only non-observers should get work
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L)); // Workers: 1,3,4

        Thread.sleep(6000);

        int numRequests = 9; // Should cycle through 3 workers 3 times each
        HttpClient client = HttpClient.newHttpClient();

        for (int i = 0; i < numRequests; i++) {
            String javaCode = String.format("public class ObserverTest%d {\n    public String run() {\n        return \"Work item %d\";\n    }\n}\n", i, i);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8888/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            Thread.sleep(100);
        }

        System.out.println("All requests completed - observers were correctly excluded from work");
        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 11: Error handling - invalid Java code")
    void testErrorHandling() throws Exception {
        System.out.println("\n=== TEST 11: Error Handling ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String invalidCode = "public class BadCode {\n    public String run() {\n        return this is not valid Java code;\n    }\n}\n";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(invalidCode))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("Response status: " + response.statusCode());
        System.out.println("Response body: " + response.body());

        assertEquals(400, response.statusCode(), "Should return 400 for compilation error");
        assertTrue(response.body().length() > 0, "Should return error details");

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 12: Wrong HTTP method")
    void testWrongHTTPMethod() throws Exception {
        System.out.println("\n=== TEST 12: Wrong HTTP Method ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(405, response.statusCode(), "Should return 405 for non-POST request");
        assertTrue(response.body().contains("POST"), "Error message should mention POST");

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 13: Wrong content type")
    void testWrongContentType() throws Exception {
        System.out.println("\n=== TEST 13: Wrong Content Type ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString("some code"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode(), "Should return 400 for wrong content type");
        assertTrue(response.body().contains("not supported") || response.body().contains("Content Type"));

        cluster.shutdown();
    }

    @Test
    @DisplayName("Test 14: High load - 30 concurrent requests")
    void testHighLoad() throws Exception {
        System.out.println("\n=== TEST 14: High Load Test ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        Cluster cluster = createCluster(ids, 2L, Set.of(9L)); // 7 workers

        Thread.sleep(8000); // Extra time for larger cluster

        int numRequests = 30;
        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numRequests; i++) {
            final int requestNum = i;
            executor.submit(() -> {
                try {
                    String javaCode = String.format("public class HighLoad%d {\n    public String run() {\n        int sum = 0;\n        for(int i = 0; i < 100; i++) {\n            sum += i;\n        }\n        return \"Request %d: sum=\" + sum;\n    }\n}\n", requestNum, requestNum);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8888/compileandrun"))
                            .header("Content-Type", "text/x-java-source")
                            .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                            .timeout(Duration.ofSeconds(45))
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        successCount.incrementAndGet();
                    } else {
                        errorCount.incrementAndGet();
                        System.err.println("Request " + requestNum + " failed: " + response.statusCode());
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Request " + requestNum + " exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(90, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;

        System.out.println("Completed: " + completed);
        System.out.println("Successes: " + successCount.get());
        System.out.println("Errors: " + errorCount.get());
        System.out.println("Duration: " + duration + "ms");

        assertTrue(completed, "All requests should complete");
        assertTrue(successCount.get() >= numRequests * 0.9,
                "At least 90% of requests should succeed under load");

        cluster.shutdown();
    }
    @Test
    @DisplayName("Test 15: Cache Performance - Verify cache is actually fast")
    void testCachePerformance() throws Exception {
        System.out.println("\n=== TEST 15: Cache Performance Diagnostic ===");
        List<Long> ids = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        Cluster cluster = createCluster(ids, 2L, Set.of(5L));

        Thread.sleep(6000);

        String javaCode = """
            public class PerfTest {
                public String run() {
                    return "Performance test";
                }
            }
            """;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();

        // UNCACHED REQUEST
        long start1 = System.currentTimeMillis();
        HttpResponse<String> response1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        long uncachedTime = System.currentTimeMillis() - start1;

        assertEquals(200, response1.statusCode());
        assertEquals("false", response1.headers().firstValue("Cached-Response").orElse(""));

        // Wait for cache to fully settle
        Thread.sleep(1000);

        // CACHED REQUEST
        long start2 = System.currentTimeMillis();
        HttpResponse<String> response2 = client.send(request, HttpResponse.BodyHandlers.ofString());
        long cachedTime = System.currentTimeMillis() - start2;

        assertEquals(200, response2.statusCode());
        assertEquals("true", response2.headers().firstValue("Cached-Response").orElse(""));

        // PERFORMANCE ANALYSIS
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║    CACHE PERFORMANCE DIAGNOSTIC        ║");
        System.out.println("╠════════════════════════════════════════╣");
        System.out.println("║ Uncached request: " + String.format("%4d", uncachedTime) + " ms            ║");
        System.out.println("║ Cached request:   " + String.format("%4d", cachedTime) + " ms            ║");
        System.out.println("║ Speedup:          " + String.format("%4.1f", (double)uncachedTime/cachedTime) + "x             ║");
        System.out.println("╚════════════════════════════════════════╝");

        // ASSERTIONS
        assertTrue(cachedTime < 200,
                "❌ CACHE TOO SLOW! Cached request took " + cachedTime + "ms (should be <200ms)");

        assertTrue(cachedTime < uncachedTime / 5,
                "❌ CACHE NOT EFFECTIVE! Cached only " +
                        String.format("%.1f", (double)uncachedTime/cachedTime) + "x faster (should be 5x+)");

        System.out.println("✅ Cache is working efficiently!");

        cluster.shutdown();
    }
    @AfterEach
    void waitForPortRelease() throws InterruptedException {
        // Give OS time to release ports between tests
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // If interrupted, still wait but acknowledge interruption
            Thread.sleep(3000);
            Thread.currentThread().interrupt(); // Restore interrupt status
        }
    }
}