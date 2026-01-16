package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage2.PeerServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LeaderElectionTest {

    private ArrayList<PeerServerImpl> servers = new ArrayList<PeerServerImpl>();
    private HashMap<Long, InetSocketAddress> peerToAddress = new HashMap<>();

    void setup(){
        for(long i = 1; i<=100; i++){
            this.peerToAddress.put(i, new InetSocketAddress(("localhost"),(int)(8000 +(i*10))));
        }
        for(Map.Entry<Long, InetSocketAddress> entry: peerToAddress.entrySet()){
            HashMap<Long, InetSocketAddress> clone = (HashMap<Long, InetSocketAddress>) peerToAddress.clone();
            clone.remove(entry.getKey());
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), clone);
            this.servers.add(server);
            server.setName("Server on port " + server.getAddress().getPort()); // Set the name directly
            server.start(); // Start the PeerServerImpl thread directly - NO wrapper!
        }
    }

    @Test
    void lookForLeader() throws InterruptedException, SocketException {
        setup();
        Thread.sleep(20000);
        long numServers = this.peerToAddress.size();
        for(PeerServerImpl server:this.servers){
            System.out.println("Server "+ server.getServerId() + " on port "+ server.getUdpPort()+" current leader vote is " + server.getCurrentLeader().getProposedLeaderID()+ ". Current state is "+ server.getPeerState());
            assertEquals(numServers, server.getCurrentLeader().getProposedLeaderID(), "Expected leader is "+ numServers +"\nActual is "+ server.getCurrentLeader().getProposedLeaderID());
        }
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        for (PeerServerImpl server : this.servers) {
            if (server != null) {
                server.shutdown(); // This will wait for workers to finish
                server.join(1000);  // Wait for the main server thread to exit
            }
        }
        this.servers.clear();
        Thread.sleep(500);
        System.gc();
    }
}