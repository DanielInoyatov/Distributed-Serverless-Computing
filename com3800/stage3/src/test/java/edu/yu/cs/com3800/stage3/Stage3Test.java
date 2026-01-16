package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class Stage3Test {
    LinkedBlockingQueue<Message> outgoingMessages;
    LinkedBlockingQueue<Message> incomingMessages;
    ArrayList<PeerServerImpl> servers = new ArrayList<>();
    HashMap<Long, InetSocketAddress> idsToAddress = new HashMap<>();
    int numServers = 3;
    InetSocketAddress myAddress  = new InetSocketAddress("localhost", 9998);
    int myPort = 9998;
    UDPMessageSender sender;
    UDPMessageReceiver receiver;
    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    void setUp() throws InterruptedException, IOException {
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();
        for(long i = 0; i < numServers; i++){
            idsToAddress.put(i, new InetSocketAddress("localhost", (int) (8000+i)));
        }
        for(long i = 0; i < numServers; i++){
            idsToAddress.remove(i);
            HashMap<Long, InetSocketAddress> map = new HashMap<>(idsToAddress);
            PeerServerImpl server = new PeerServerImpl((int) (8000+i), 0, (long)(i), map);
            servers.add(server);
            idsToAddress.put(i, server.getAddress());
        }

        startServers();
        printServerStates();
    }
    void printServerStates(){
        for(PeerServerImpl server: servers){
            System.out.println("Server " + server.getServerId() +"'s leader is "+ server.getCurrentLeader().getProposedLeaderID()+" and it's state is " + server.getPeerState());
        }
    }
    void startServers() throws InterruptedException, IOException {
        for(PeerServerImpl server : servers){
            server.start();
        }
        Thread.sleep(1000);
        boolean doneCreating = false;
        while (!doneCreating) {
            Thread.sleep(500);
            doneCreating = true;
            for(PeerServer server : servers){
                if(server.getPeerState()== PeerServer.ServerState.LOOKING){
                    doneCreating = false;
                    break;
                }
            }
        }
        sender = new UDPMessageSender(this.outgoingMessages, myPort);
        sender.start();
        receiver = new UDPMessageReceiver(incomingMessages, myAddress, myPort, null);
        receiver.start();
    }
    private void stopServers(){
        for(PeerServerImpl server: servers){
            server.shutdown();
        }
    }

    private void sendMessage(String code) throws InterruptedException {
        Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", 8000+numServers-1);
        this.outgoingMessages.put(msg);
    }

    @Test
    void startTest() throws InterruptedException, IOException {
        setUp();
    }

    @Test
    void testClusterFormsAndElectsLeader() throws InterruptedException, IOException {
        setUp();
        for(PeerServerImpl server: servers){
            assertEquals(new Vote(numServers-1, 0),  server.getCurrentLeader());
        }
        stopServers();
    }

    @Test
    void testSingleWorkRequest() throws InterruptedException, IOException {
        setUp();
        String code = validClass.replace("world!", "world! from code version " + 0);
        sendMessage(code);

        assertEquals(this.outgoingMessages.size(), 1);

        Message msg = incomingMessages.take();
        assertEquals(new String(msg.getMessageContents()), "Hello world! from code version " + 0);
        stopServers();
    }

    @Test
    void testWorkRequestWithException() throws IOException, InterruptedException {
        String code ="invalid";
        setUp();
        sendMessage(code);
        Message msg = this.incomingMessages.take();
        System.out.println(new String(msg.getMessageContents()));
        assertTrue(new String(msg.getMessageContents()).contains("Exception"));
        stopServers();
    }

    @Test
    void testRoundRobinDistribution() throws InterruptedException, IOException {
        setUp();
        for(int i = 0; i < numServers*2; i++){
            System.out.println("Sending " + i + " requests to round robin");
            String code =  validClass.replace("world!", "world! from code version " + i);
            sendMessage(code);//will manually check logger to make sure everything went to the worker it needed to go to
        }
        Thread.sleep(3000);
        stopServers();
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        for (PeerServerImpl server : this.servers) {
            if (server != null) {
                server.shutdown(); // This will wait for workers to finish
                server.join(1500);  // Wait for the main server thread to exit
            }
        }
        this.servers.clear();
        this.receiver.shutdown();
        this.sender.shutdown();
        Thread.sleep(1500);
        System.gc();
    }
}