package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Stage4PeerServerDemo {
    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\"\n    }\n}\n";

    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private int[] ports = {8010, 8020, 8030, 8040, 8050,8060};//, 8060, 8070, 8080, 8090, 8100
    private int leaderPort= 8040;
    private int gatewayPort = 8060;
    private int myPort = 9997;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ArrayList<PeerServer> servers;
    HashSet<Integer> observerPorts =  new HashSet<>();

    public Stage4PeerServerDemo() throws Exception {
        //step 1: create sender & sending queue
        this.outgoingMessages = new LinkedBlockingQueue<>();
        TCPMessageSender sender = new TCPMessageSender(this.outgoingMessages, this.myPort, "localHost", this.gatewayPort+2);
        this.incomingMessages = new LinkedBlockingQueue<>();
        TCPMessageReceiver receiver = new TCPMessageReceiver(this.incomingMessages, this.myPort);
        //step 2: create servers
        createServers();
        //step2.1: wait for servers to get started

        try {
            long start = System.currentTimeMillis();
            Thread.sleep(1000);
            boolean doneCreating = false;
            while (!doneCreating) {
                Thread.sleep(1000);
                doneCreating = true;
                for(PeerServer server : servers){
                    if(server.getPeerState()== PeerServer.ServerState.LOOKING){
                        doneCreating = false;
                        break;
                    }
                }
            }
            System.out.println("It took "+ (System.currentTimeMillis()-start)/1000.0 + " seconds to start up.");
        }
        catch (InterruptedException e) {
        }
        printLeaders();
        //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
        for (int i = 0; i < this.ports.length; i++) {
            String code = this.validClass.replace("world!", "world! from code version " + i);
            sendMessage(code);

        }
        Util.startAsDaemon(sender, "Sender thread");
        Util.startAsDaemon(receiver, "Receiver thread");
        //step 4: validate responses from leader

        printResponses();

        //step 5: stop servers
        Thread.sleep(3000);
        stopServers();
    }

    private void printLeaders() {
        for (PeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }
    }

    private void stopServers() {
        for (PeerServer server : this.servers) {
            server.shutdown();
        }
    }

    private void printResponses() throws Exception {
        String completeResponse = "";
        for (int i = 0; i < this.ports.length; i++) {
            Message msg = this.incomingMessages.take();
            String response = new String(msg.getMessageContents());
            completeResponse += "Response to request " + msg.getRequestID() + ":\n" + response + "\n\n";

        }
        System.out.println(completeResponse);
    }

    private void sendMessage(String code) throws InterruptedException {
        Message msg = new Message(Message.MessageType.WORK, code.getBytes(), "localhost", this.myPort, "localhost", this.gatewayPort+2);
        this.outgoingMessages.put(msg);
    }

    private void createServers() throws IOException, InterruptedException {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }
        //create servers
        this.servers = new ArrayList<>();
        int numObservers = 2;
        int curServer = 1;
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(curServer <=this.ports.length-numObservers && entry.getValue().getPort()!=8020 && entry.getValue().getPort()!=8060){// && entry.getValue().getPort() != 8020 && entry.getValue().getPort() != 8050{
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
                map.remove(entry.getKey());
                PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 5L, numObservers+1);
                this.servers.add(server);
                server.start();
                Thread.sleep(50);
                curServer++;
            }
            else if (entry.getValue().getPort()==8060) {
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
                map.remove(entry.getKey());
                GatewayPeerServerImpl server = new GatewayPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 5L, numObservers+1);
                this.servers.add(server);
                server.start();
                Thread.sleep(50);
                curServer++;
            }
            else{
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
                map.remove(entry.getKey());
                PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 5L, numObservers);
                server.setPeerState(PeerServer.ServerState.OBSERVER);
                this.servers.add(server);
                this.observerPorts.add(entry.getValue().getPort());
                server.start();
                Thread.sleep(50);
                curServer++;
            }
        }

        this.leaderPort = this.ports[this.ports.length -numObservers];
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        new Stage4PeerServerDemo();
        long endTime = System.currentTimeMillis();
        System.out.println("Server started in " + (endTime - time) + " ms");
    }
}