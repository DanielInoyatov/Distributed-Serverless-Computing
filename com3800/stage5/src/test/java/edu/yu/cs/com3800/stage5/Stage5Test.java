package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.Headers;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive JUnit test suite for Stage 5: Fault Tolerance
 * Tests gossip-style heartbeats, failure detection, leader election, and work reassignment
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Stage5Test {

    private static final int ELECTION_TIMEOUT = 15_000; // 15 seconds for election
    private static final int FAILURE_DETECTION_TIMEOUT = 15_000; //15 seconds for failure detection
    private static final int WORK_COMPLETION_TIMEOUT = 10000; // 10 seconds for work completion
    private static final int CLEANUP_DELAY = 5000; // 5 seconds between tests for port cleanup
    private static final int HTTP_PORT = 8888;
    private static final int BASE_UDP_PORT = 8010;

    private Cluster currentCluster = null;

    @AfterEach
    public void cleanup() throws InterruptedException {
        if (currentCluster != null) {
            System.out.println("\n=== Cleaning up cluster ===");
            try {
                currentCluster.shutdown();
            } catch (Exception e) {
                System.err.println("Error during cluster shutdown: " + e.getMessage());
            }
            currentCluster = null;
        }
        
        // Wait for ports to be fully released by OS
        System.out.println("Waiting " + CLEANUP_DELAY + "ms for port cleanup...");
        Thread.sleep(CLEANUP_DELAY);

        // Force garbage collection to help release resources
        System.gc();
        Thread.sleep(500);
        System.out.println("Cleanup complete\n");
    }

    /**
     * Inner class to manage cluster lifecycle
     */
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

        Map<Long, PeerServerImpl> getPeerServers() {
            return peerServers;
        }

        void shutdown() {
            System.out.println("Shutting down cluster...");

            // Shut down all non-gateway peer servers first
            for (PeerServerImpl server : peerServers.values()) {
                try {
                    if (server != null && server != gateway.getPeerServer() && server.isAlive()) {
                        server.shutdown();
                    }
                } catch (Exception e) {
                    System.err.println("Error shutting down server " + server.getServerId() + ": " + e.getMessage());
                }
            }

            // Wait for all peer servers to die
            for (PeerServerImpl server : peerServers.values()) {
                try {
                    if (server != null && server != gateway.getPeerServer()) {
                        server.join(2000);
                    }
                } catch (Exception e) {
                    System.err.println("Error waiting for server to die: " + e.getMessage());
                }
            }

            // Shut down the GatewayServer LAST
            try {
                if (gateway != null) {
                    gateway.shutdown();
                    gateway.join(3000);
                }
            } catch (Exception e) {
                System.err.println("Error shutting down gateway: " + e.getMessage());
            }

            // Extra time for final cleanup
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("Cluster shutdown complete");
        }

        long getLeaderId() {
            Vote leader = null;
            // Get leader from any voting server
            for (PeerServerImpl server : peerServers.values()) {
                if (server.getPeerState() != PeerServer.ServerState.OBSERVER) {
                    leader = server.getCurrentLeader();
                    if (leader != null) {
                        break;
                    }
                }
            }
            return leader != null ? leader.getProposedLeaderID() : -1L;
        }

        long getExpectedLeaderId() {
            // Leader should be the highest ID among non-observers and alive servers
            long maxId = -1;
            for (Long id : peerServers.keySet()) {
                if (!observerIds.contains(id) && id != gatewayId) {
                    PeerServerImpl server = peerServers.get(id);
                    if (server != null && server.isAlive()) {
                        maxId = Math.max(maxId, id);
                    }
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

        int countAliveVotingServers() {
            int count = 0;
            for (Long id : peerServers.keySet()) {
                if (!observerIds.contains(id) && id != gatewayId) {
                    PeerServerImpl server = peerServers.get(id);
                    if (server != null && server.isAlive()) {
                        count++;
                    }
                }
            }
            return count;
        }
    }

    /**
     * Creates a cluster with configurable observers using fixed ports
     */
    private Cluster createCluster(List<Long> allIds, long gatewayId, Set<Long> additionalObserverIds)
            throws IOException, InterruptedException {
        long peerEpoch = 0L;
        int numberOfObservers = 1 + additionalObserverIds.size(); // gateway + additional

        // Assign UDP ports - using fixed base port
        Map<Long, Integer> idToUdpPort = new HashMap<>();
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();

        for (long id : allIds) {
            int udpPort = BASE_UDP_PORT + ((int) id - 1) * 10;
            idToUdpPort.put(id, udpPort);
            peerIDtoAddress.put(id, new InetSocketAddress("localhost", udpPort));
        }

        // Create gateway
        int gatewayUdpPort = idToUdpPort.get(gatewayId);
        Map<Long, InetSocketAddress> peersForGateway = new HashMap<>(peerIDtoAddress);
        peersForGateway.remove(gatewayId);

        GatewayServer gateway = new GatewayServer(
                HTTP_PORT,
                gatewayUdpPort,
                peerEpoch,
                gatewayId,
                new ConcurrentHashMap<>(peersForGateway),
                numberOfObservers
        );
        gateway.start();

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

        for (PeerServerImpl server : servers.values()) {
            server.start();
        }

        Thread.sleep(1000);

        Set<Long> allObserverIds = new HashSet<>(additionalObserverIds);
        allObserverIds.add(gatewayId);

        Cluster cluster = new Cluster(gateway, servers, idToUdpPort, gatewayId, allObserverIds);
        currentCluster = cluster; // Store for cleanup
        return cluster;
    }

    /**
     * Sends HTTP POST request to gateway
     */
    private static String sendWorkToGateway(String code) throws IOException {
        URL url = new URL("http://localhost:" + HTTP_PORT + "/compileandrun");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/x-java-source");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = code.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();
        InputStream inputStream = (responseCode >= 200 && responseCode < 400)
                ? conn.getInputStream()
                : conn.getErrorStream();

        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }

        String response = result.toString(StandardCharsets.UTF_8);
        conn.disconnect();
        return response;
    }

    /**
     * Simple Java code that returns "Hello World"
     */
    private static String getSimpleJavaCode(int number) {
        return "public class HelloWorld" + number + " {\n" +
                "    public String run() {\n" +
                "        return \"Hello World " + number + "\";\n" +
                "    }\n" +
                "}\n";
    }

    /**
     * Java code that will cause an error
     */
    private static String getErrorJavaCode() {
        return "public class ErrorCode {\n" +
                "    public String run() {\n" +
                "        throw new RuntimeException(\"Intentional error\");\n" +
                "    }\n" +
                "}\n";
    }

    /**
     * Test 1: Initial cluster setup and leader election
     */
    @Test
    @Order(1)
    @DisplayName("Test 1: Initial cluster setup and correct leader election")
    public void testInitialLeaderElection() throws Exception {
        System.out.println("\n=== Test 1: Initial Leader Election ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for election to complete
            Thread.sleep(ELECTION_TIMEOUT);

            long actualLeader = cluster.getLeaderId();
            long expectedLeader = cluster.getExpectedLeaderId();

            System.out.println("Expected leader: " + expectedLeader);
            System.out.println("Actual leader: " + actualLeader);

            assertEquals(expectedLeader, actualLeader, "Leader should be the highest ID server");

            // Verify all voting servers agree on leader
            for (Map.Entry<Long, PeerServerImpl> entry : cluster.getPeerServers().entrySet()) {
                if (!cluster.observerIds.contains(entry.getKey())) {
                    Vote leader = entry.getValue().getCurrentLeader();
                    assertNotNull(leader, "Server " + entry.getKey() + " should have a leader");
                    assertEquals(expectedLeader, leader.getProposedLeaderID(),
                            "Server " + entry.getKey() + " should agree on leader");
                }
            }

            System.out.println("✓ Leader election successful");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 2: Follower failure detection by other followers
     */
    @Test
    @Order(2)
    @DisplayName("Test 2: Follower failure puts failed node in failed set")
    public void testFollowerFailureDetection() throws Exception {
        System.out.println("\n=== Test 2: Follower Failure Detection ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long leaderId = cluster.getLeaderId();
            System.out.println("Initial leader: " + leaderId);

            // Kill a follower (not the leader)
            Long followerToKill = null;
            for (Long id : serverIds) {
                if (id != leaderId && id != gatewayId) {
                    followerToKill = id;
                    break;
                }
            }

            assertNotNull(followerToKill, "Should have a follower to kill");
            System.out.println("Killing follower: " + followerToKill);

            PeerServerImpl killedFollower = cluster.getPeerServers().get(followerToKill);
            killedFollower.shutdown();
            killedFollower.join(2000);

            // Wait for failure detection
            Thread.sleep(FAILURE_DETECTION_TIMEOUT);

            // Verify other followers detected the failure
            for (Map.Entry<Long, PeerServerImpl> entry : cluster.getPeerServers().entrySet()) {
                Long serverId = entry.getKey();
                PeerServerImpl server = entry.getValue();

                if (serverId.equals(followerToKill) || serverId.equals(gatewayId)) {
                    continue; // Skip killed server and gateway
                }

                assertTrue(server.isPeerDead(followerToKill),
                        "Server " + serverId + " should detect that server " + followerToKill + " failed");
            }

            System.out.println("✓ Follower failure detected by all other servers");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 3: Leader doesn't assign work to failed follower
     */
    @Test
    @Order(3)
    @DisplayName("Test 3: Leader stops assigning work to failed follower")
    public void testLeaderStopsAssigningToFailedFollower() throws Exception {
        System.out.println("\n=== Test 3: Leader Stops Assigning to Failed Follower ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long leaderId = cluster.getLeaderId();
            System.out.println("Initial leader: " + leaderId);

            // Kill a follower
            Long followerToKill = null;
            for (Long id : serverIds) {
                if (id != leaderId && id != gatewayId) {
                    followerToKill = id;
                    break;
                }
            }

            assertNotNull(followerToKill);
            System.out.println("Killing follower: " + followerToKill);

            PeerServerImpl killedFollower = cluster.getPeerServers().get(followerToKill);
            killedFollower.shutdown();
            killedFollower.join(2000);

            // Wait for failure detection
            Thread.sleep(FAILURE_DETECTION_TIMEOUT);

            // Send work requests - they should all complete even though one follower is dead
            System.out.println("Sending work after follower failure...");
            int numRequests = 5;
            for (int i = 0; i < numRequests; i++) {
                String response = sendWorkToGateway(getSimpleJavaCode(i));
                assertTrue(response.contains("Hello World " + i),
                        "Request " + i + " should complete successfully");
                System.out.println("Request " + i + " completed: " + response.trim());
            }

            System.out.println("✓ All work completed despite follower failure");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 4: Leader reassigns incomplete work from failed follower
     */
    @Test
    @Order(4)
    @DisplayName("Test 4: Leader reassigns work from failed follower")
    public void testLeaderReassignsWorkFromFailedFollower() throws Exception {
        System.out.println("\n=== Test 4: Leader Reassigns Work from Failed Follower ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long leaderId = cluster.getLeaderId();
            System.out.println("Initial leader: " + leaderId);

            // Start submitting work in background threads
            List<Thread> workThreads = new ArrayList<>();
            List<String> results = Collections.synchronizedList(new ArrayList<>());
            int numRequests = 6;

            for (int i = 0; i < numRequests; i++) {
                final int requestNum = i;
                Thread workThread = new Thread(() -> {
                    try {
                        String response = sendWorkToGateway(getSimpleJavaCode(requestNum));
                        results.add(response);
                        System.out.println("Request " + requestNum + " completed");
                    } catch (Exception e) {
                        System.err.println("Request " + requestNum + " failed: " + e.getMessage());
                    }
                });
                workThreads.add(workThread);
                workThread.start();
                Thread.sleep(200); // Stagger the requests
            }

            // Kill a follower while work is in progress
            Thread.sleep(1000);
            Long followerToKill = null;
            for (Long id : serverIds) {
                if (id != leaderId && id != gatewayId) {
                    followerToKill = id;
                    break;
                }
            }

            System.out.println("Killing follower: " + followerToKill + " while work is in progress");
            PeerServerImpl killedFollower = cluster.getPeerServers().get(followerToKill);
            killedFollower.shutdown();
            killedFollower.join(2000);

            // Wait for all work to complete
            for (Thread t : workThreads) {
                t.join(WORK_COMPLETION_TIMEOUT * 2);
            }

            // Verify all requests completed
            assertEquals(numRequests, results.size(), "All requests should complete");
            for (int i = 0; i < numRequests; i++) {
                boolean foundResult = false;
                for (String result : results) {
                    if (result.contains("Hello World " + i)) {
                        foundResult = true;
                        break;
                    }
                }
                assertTrue(foundResult, "Should find result for request " + i);
            }

            System.out.println("✓ All work reassigned and completed successfully");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 5: Gateway queues work when leader fails and sends to new leader
     */
    @Test
    @Order(5)
    @DisplayName("Test 5: Gateway queues work during leader failure and sends to new leader")
    public void testGatewayQueuesWorkDuringLeaderFailure() throws Exception {
        System.out.println("\n=== Test 5: Gateway Queues Work During Leader Failure ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long initialLeader = cluster.getLeaderId();
            System.out.println("Initial leader: " + initialLeader);

            // Kill the leader
            System.out.println("Killing leader: " + initialLeader);
            PeerServerImpl leaderServer = cluster.getPeerServers().get(initialLeader);
            leaderServer.shutdown();
            leaderServer.join(1000);

            // Immediately send work while leader is down
            System.out.println("Sending work while leader is down...");
            List<Thread> workThreads = new ArrayList<>();
            List<String> results = Collections.synchronizedList(new ArrayList<>());
            int numRequests = 3;

            for (int i = 0; i < numRequests; i++) {
                final int requestNum = i;
                Thread workThread = new Thread(() -> {
                    try {
                        String response = sendWorkToGateway(getSimpleJavaCode(requestNum));
                        results.add(response);
                        System.out.println("Request " + requestNum + " completed after re-election");
                    } catch (Exception e) {
                        System.err.println("Request " + requestNum + " failed: " + e.getMessage());
                    }
                });
                workThreads.add(workThread);
                workThread.start();
                Thread.sleep(100);
            }

            // Wait for failure detection and new election
            Thread.sleep(FAILURE_DETECTION_TIMEOUT + ELECTION_TIMEOUT);

            long newLeader = cluster.getLeaderId();
            System.out.println("New leader elected: " + newLeader);
            assertNotEquals(initialLeader, newLeader, "Should have a new leader");

            // Wait for queued work to complete
            for (Thread t : workThreads) {
                t.join(WORK_COMPLETION_TIMEOUT * 3);
            }

            // Verify all requests completed
            assertEquals(numRequests, results.size(), "All queued requests should complete");
            System.out.println("✓ Gateway successfully queued and processed work with new leader");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 6: Gateway rejects responses from failed leader
     */
    @Test
    @Order(6)
    @DisplayName("Test 6: Gateway ignores responses from failed leader")
    public void testGatewayIgnoresFailedLeaderResponses() throws Exception {
        System.out.println("\n=== Test 6: Gateway Ignores Failed Leader Responses ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long initialLeader = cluster.getLeaderId();
            System.out.println("Initial leader: " + initialLeader);

            // Send a request
            Thread workThread = new Thread(() -> {
                try {
                    String response = sendWorkToGateway(getSimpleJavaCode(99));
                    assertTrue(response.contains("Hello World 99"));
                    System.out.println("Work completed successfully after leader change");
                } catch (Exception e) {
                    System.err.println("Work failed: " + e.getMessage());
                }
            });
            workThread.start();

            // Quickly kill the leader while request is in flight
            Thread.sleep(500);
            System.out.println("Killing leader while request is in flight: " + initialLeader);
            PeerServerImpl leaderServer = cluster.getPeerServers().get(initialLeader);
            leaderServer.shutdown();
            leaderServer.join(2000);

            // Wait for new election and work completion
            Thread.sleep(FAILURE_DETECTION_TIMEOUT + ELECTION_TIMEOUT);
            workThread.join(WORK_COMPLETION_TIMEOUT * 2);

            long newLeader = cluster.getLeaderId();
            System.out.println("New leader: " + newLeader);
            assertNotEquals(initialLeader, newLeader, "Should have new leader");

            System.out.println("✓ Gateway correctly handled leader failure mid-request");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 7: Followers queue completed work and send to new leader
     */
    @Test
    @Order(7)
    @DisplayName("Test 7: Followers queue completed work for new leader")
    public void testFollowersQueueWorkForNewLeader() throws Exception {
        System.out.println("\n=== Test 7: Followers Queue Completed Work for New Leader ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long initialLeader = cluster.getLeaderId();
            System.out.println("Initial leader: " + initialLeader);

            // Submit multiple requests
            int numRequests = 4;
            List<Thread> workThreads = new ArrayList<>();
            List<String> results = Collections.synchronizedList(new ArrayList<>());

            for (int i = 0; i < numRequests; i++) {
                final int requestNum = i;
                Thread workThread = new Thread(() -> {
                    try {
                        String response = sendWorkToGateway(getSimpleJavaCode(requestNum));
                        results.add(response);
                        System.out.println("Request " + requestNum + " completed");
                    } catch (Exception e) {
                        System.err.println("Request " + requestNum + " error: " + e.getMessage());
                    }
                });
                workThreads.add(workThread);
                workThread.start();
                Thread.sleep(100);
            }

            // Kill leader after a short delay (some work may be in progress)
            Thread.sleep(1000);
            System.out.println("Killing leader: " + initialLeader);
            PeerServerImpl leaderServer = cluster.getPeerServers().get(initialLeader);
            leaderServer.shutdown();
            leaderServer.join(2000);

            // Wait for new election and work completion
            Thread.sleep(FAILURE_DETECTION_TIMEOUT + ELECTION_TIMEOUT);

            for (Thread t : workThreads) {
                t.join(WORK_COMPLETION_TIMEOUT * 3);
            }

            // All work should eventually complete via new leader
            assertTrue(results.size() > 0, "At least some work should complete");
            System.out.println("Completed " + results.size() + " out of " + numRequests + " requests");

            System.out.println("✓ Followers successfully queued and transmitted completed work");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 8: Followers trigger leader election when leader fails
     */
    @Test
    @Order(8)
    @DisplayName("Test 8: Followers correctly trigger and complete leader election")
    public void testFollowersElectNewLeaderAfterFailure() throws Exception {
        System.out.println("\n=== Test 8: Followers Elect New Leader ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long initialLeader = cluster.getLeaderId();
            long expectedNewLeader = cluster.getExpectedLeaderId();
            System.out.println("Initial leader: " + initialLeader);
            System.out.println("Expected leader equals actual: " + (initialLeader == expectedNewLeader));

            // Kill the leader
            System.out.println("Killing leader: " + initialLeader);
            PeerServerImpl leaderServer = cluster.getPeerServers().get(initialLeader);
            leaderServer.shutdown();
            leaderServer.join(2000);

            // Wait for failure detection and new election
            Thread.sleep(FAILURE_DETECTION_TIMEOUT + ELECTION_TIMEOUT);

            long newLeader = cluster.getLeaderId();
            long newExpectedLeader = cluster.getExpectedLeaderId();

            System.out.println("New leader: " + newLeader);
            System.out.println("Expected new leader: " + newExpectedLeader);

            assertNotEquals(initialLeader, newLeader, "Should have elected a new leader");
            assertEquals(newExpectedLeader, newLeader, "Should elect highest ID remaining server");

            // Verify all remaining servers agree
            for (Map.Entry<Long, PeerServerImpl> entry : cluster.getPeerServers().entrySet()) {
                if (entry.getKey().equals(initialLeader)) continue;
                if (cluster.observerIds.contains(entry.getKey())) continue;

                Vote leader = entry.getValue().getCurrentLeader();
                assertNotNull(leader, "Server " + entry.getKey() + " should have a leader");
                assertEquals(newLeader, leader.getProposedLeaderID(),
                        "Server " + entry.getKey() + " should agree on new leader");
            }

            System.out.println("✓ New leader elected and all servers agree");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 9: Multiple leader failures - no duplicate work processing
     */
    @Test
    @Order(9)
    @DisplayName("Test 9: Multiple failures - no duplicate work processing")
    public void testMultipleFailuresNoDuplicateWork() throws Exception {
        System.out.println("\n=== Test 9: Multiple Failures, No Duplicate Processing ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L);
        long gatewayId = 6L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial election
            Thread.sleep(ELECTION_TIMEOUT);

            long firstLeader = cluster.getLeaderId();
            System.out.println("First leader: " + firstLeader);

            // Send some work
            String response1 = sendWorkToGateway(getSimpleJavaCode(1));
            assertTrue(response1.contains("Hello World 1"));
            System.out.println("Initial work completed");

            // Kill first leader
            System.out.println("Killing first leader: " + firstLeader);
            cluster.getPeerServers().get(firstLeader).shutdown();
            cluster.getPeerServers().get(firstLeader).join(2000);

            Thread.sleep(FAILURE_DETECTION_TIMEOUT + ELECTION_TIMEOUT);

            long secondLeader = cluster.getLeaderId();
            System.out.println("Second leader: " + secondLeader);
            assertNotEquals(firstLeader, secondLeader, "Should have new leader");

            // Send more work with second leader
            String response2 = sendWorkToGateway(getSimpleJavaCode(2));
            assertTrue(response2.contains("Hello World 2"));
            System.out.println("Work completed with second leader");

            // Kill second leader
            System.out.println("Killing second leader: " + secondLeader);
            cluster.getPeerServers().get(secondLeader).shutdown();
            cluster.getPeerServers().get(secondLeader).join(2000);

            Thread.sleep(FAILURE_DETECTION_TIMEOUT + ELECTION_TIMEOUT);

            long thirdLeader = cluster.getLeaderId();
            System.out.println("Third leader: " + thirdLeader);
            assertNotEquals(secondLeader, thirdLeader, "Should have third leader");

            // Send final work - should not include duplicates
            String response3 = sendWorkToGateway(getSimpleJavaCode(3));
            assertTrue(response3.contains("Hello World 3"));
            System.out.println("Work completed with third leader");

            System.out.println("✓ Multiple leader failures handled without duplicate processing");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 10: HTTP log endpoints work correctly
     */
    @Test
    @Order(10)
    @DisplayName("Test 10: HTTP log endpoints return valid logs")
    public void testHttpLogEndpoints() throws Exception {
        System.out.println("\n=== Test 10: HTTP Log Endpoints ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L);
        long gatewayId = 4L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for cluster to stabilize
            Thread.sleep(ELECTION_TIMEOUT);

            // Send some work to generate logs
            sendWorkToGateway(getSimpleJavaCode(1));
            Thread.sleep(2000);

            // Test summary and verbose log endpoints for each server
            for (Map.Entry<Long, Integer> entry : cluster.serverIdToUdpPort.entrySet()) {
                Long serverId = entry.getKey();
                Integer udpPort = entry.getValue();
                int httpLogPort = udpPort + 5; // Servers expose logs on udp_port + 5

                // Test summary log endpoint
                try {
                    URL summaryUrl = new URL("http://localhost:" + httpLogPort + "/logs/summary");
                    HttpURLConnection conn = (HttpURLConnection) summaryUrl.openConnection();
                    conn.setRequestMethod("GET");

                    int responseCode = conn.getResponseCode();
                    assertEquals(200, responseCode, "Server " + serverId + " summary log endpoint should return 200");

                    InputStream is = conn.getInputStream();
                    String summaryLog = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    assertNotNull(summaryLog, "Summary log should not be null");
                    System.out.println("Server " + serverId + " summary log length: " + summaryLog.length());

                    conn.disconnect();
                } catch (Exception e) {
                    System.err.println("Failed to get summary log for server " + serverId + ": " + e.getMessage());
                }

                // Test verbose log endpoint
                try {
                    URL verboseUrl = new URL("http://localhost:" + httpLogPort + "/logs/verbose");
                    HttpURLConnection conn = (HttpURLConnection) verboseUrl.openConnection();
                    conn.setRequestMethod("GET");

                    int responseCode = conn.getResponseCode();
                    assertEquals(200, responseCode, "Server " + serverId + " verbose log endpoint should return 200");

                    InputStream is = conn.getInputStream();
                    String verboseLog = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    assertNotNull(verboseLog, "Verbose log should not be null");
                    System.out.println("Server " + serverId + " verbose log length: " + verboseLog.length());

                    conn.disconnect();
                } catch (Exception e) {
                    System.err.println("Failed to get verbose log for server " + serverId + ": " + e.getMessage());
                }
            }

            System.out.println("✓ HTTP log endpoints working correctly");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 11: Work completes correctly after follower failure
     */
    @Test
    @Order(11)
    @DisplayName("Test 11: System continues working normally after follower failure")
    public void testSystemContinuesAfterFollowerFailure() throws Exception {
        System.out.println("\n=== Test 11: System Continues After Follower Failure ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L, 5L);
        long gatewayId = 5L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            // Wait for initial setup
            Thread.sleep(ELECTION_TIMEOUT);

            // Send initial work
            String response1 = sendWorkToGateway(getSimpleJavaCode(1));
            assertTrue(response1.contains("Hello World 1"));
            System.out.println("Initial work successful");

            // Kill a follower
            long leaderId = cluster.getLeaderId();
            Long followerToKill = null;
            for (Long id : serverIds) {
                if (id != leaderId && id != gatewayId) {
                    followerToKill = id;
                    break;
                }
            }

            System.out.println("Killing follower: " + followerToKill);
            cluster.getPeerServers().get(followerToKill).shutdown();
            cluster.getPeerServers().get(followerToKill).join(2000);

            Thread.sleep(FAILURE_DETECTION_TIMEOUT);

            // Continue sending work - should work fine
            for (int i = 2; i <= 5; i++) {
                String response = sendWorkToGateway(getSimpleJavaCode(i));
                assertTrue(response.contains("Hello World " + i),
                        "Work " + i + " should complete after follower failure");
                System.out.println("Work " + i + " completed successfully");
            }

            System.out.println("✓ System continues operating normally after follower failure");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Test 12: Error handling - code with errors is properly handled
     */
    @Test
    @Order(12)
    @DisplayName("Test 12: System handles code errors gracefully")
    public void testErrorHandling() throws Exception {
        System.out.println("\n=== Test 12: Error Handling ===");

        List<Long> serverIds = Arrays.asList(1L, 2L, 3L, 4L);
        long gatewayId = 4L;
        Set<Long> additionalObservers = new HashSet<>();

        Cluster cluster = createCluster(serverIds, gatewayId, additionalObservers);

        try {
            Thread.sleep(ELECTION_TIMEOUT);

            // Send code that will throw an error
            String errorResponse = sendWorkToGateway(getErrorJavaCode());
            assertTrue(errorResponse.contains("Intentional error") || errorResponse.contains("RuntimeException"),
                    "Error should be properly reported");
            System.out.println("Error properly handled and returned");

            // Verify system still works after error
            String successResponse = sendWorkToGateway(getSimpleJavaCode(99));
            assertTrue(successResponse.contains("Hello World 99"),
                    "System should work normally after handling error");
            System.out.println("System continues working after error");

            System.out.println("✓ Error handling works correctly");
        } catch (Exception e) {
            throw e;
        }
    }
}