package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class  PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final InetSocketAddress myAddress;
    protected final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessagesUDP;
    private final LinkedBlockingQueue<Message> incomingMessagesUDP;
    private final LinkedBlockingQueue<Message> outgoingMessagesHeartbeatUDP;
    private final LinkedBlockingQueue<Message> incomingMessagesHeartbeatUDP;
    protected final LinkedBlockingQueue<Message> incomingMessagesTCP;//made this protected so the gatewaypeerserverimpl can access it, is that an issue?
    protected final LinkedBlockingQueue<Message> outgoingMessagesTCP;
    protected Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    protected volatile ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;//based of the demo code, the peerserver will never have its owm key value in this map
    protected Logger logger;
    private HttpServer logHttpServer;
    private Thread serverThread;
    private UDPMessageSender udpSender;
    private UDPMessageReceiver udpReceiver;
    private UDPMessageSender udpHeartbeatSender;
    private UDPMessageReceiver udpHeartbeatReceiver;
    protected TCPMessageSender tcpSender;
    protected TCPMessageReceiver tcpReceiver;
    protected long gatewayID;
    private int numberOfObservers;
    protected volatile InetSocketAddress leaderAddress;
    private Thread tcpReceiverThread;
    private Thread tcpSenderThread;
    private ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock = new ConcurrentHashMap<>();
    private GossipSender gossipSender;
    private GossipReceiver gossipReceiver;
    private Set<InetSocketAddress> failedServersAdresses = ConcurrentHashMap.newKeySet();
    protected Set<Long> failedServersIDs = ConcurrentHashMap.newKeySet();
    private LinkedBlockingQueue<Message> completedWorkQueue = new LinkedBlockingQueue<>();
    private Thread detectorThread;
    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long,InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers){
        //code here...
        this.myPort = udpPort;
        this.myAddress =  new InetSocketAddress("localhost", this.myPort);
        this.state = ServerState.LOOKING;
        this.shutdown = false;
        this.outgoingMessagesUDP = new LinkedBlockingQueue<>();
        this.incomingMessagesUDP = new LinkedBlockingQueue<>();
        this.incomingMessagesTCP = new LinkedBlockingQueue<>();
        this.outgoingMessagesTCP = new LinkedBlockingQueue<>();
        this.outgoingMessagesHeartbeatUDP = new LinkedBlockingQueue<>();
        this.incomingMessagesHeartbeatUDP = new LinkedBlockingQueue<>();
        this.id = serverID;
        this.peerEpoch = peerEpoch;
        this.currentLeader = new Vote(this.id, this.peerEpoch);
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress);
        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;
        //step 1: create and run thread that sends broadcast messages
        this.udpSender = new UDPMessageSender(this.outgoingMessagesUDP, this.myPort);
        this.udpSender.start();
        this.udpHeartbeatSender = new UDPMessageSender(this.outgoingMessagesHeartbeatUDP, this.myPort+1);
        this.udpHeartbeatSender.start();
        this.heartbeatVectorClock = new ConcurrentHashMap<>();
        for(Long id: this.peerIDtoAddress.keySet()){
            this.heartbeatVectorClock.put(id, new Heartbeat(id, 0));
        }
        this.heartbeatVectorClock.put(this.id, new Heartbeat(this.id, 0));
        //step 2: create and run thread that listens for messages sent to this server
        try {
            this.udpReceiver= new UDPMessageReceiver(this.incomingMessagesUDP, this.myAddress, this.myPort, this);
            this.udpReceiver.start();
            this.udpHeartbeatReceiver = new UDPMessageReceiver(this.incomingMessagesHeartbeatUDP, new InetSocketAddress(this.myAddress.getHostName(), this.myAddress.getPort()+1), this.myPort+1, this);
            this.udpHeartbeatReceiver.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try{
            if(!serverID.equals(gatewayID)){
                this.logger = initializeLogging(PeerServerImpl.class.getCanonicalName() + "-on-port-" + this.myPort);
            }
            else{
                this.logger = initializeLogging(GatewayPeerServerImpl.class.getCanonicalName() + "-on-port-" + this.myPort);

            }
            this.logger.setUseParentHandlers(false);

        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        try{
            this.gossipSender = new GossipSender(heartbeatVectorClock,outgoingMessagesHeartbeatUDP,peerIDtoAddress,this,this.id);
            this.gossipReceiver = new GossipReceiver(heartbeatVectorClock, incomingMessagesHeartbeatUDP,peerIDtoAddress,this,this.id);
            this.gossipSender.start();
            this.gossipReceiver.start();
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
        setName("PeerServerImpl-" + this.myPort);
        try {
            this.logHttpServer = HttpServer.create(new InetSocketAddress(this.myPort + 5), 0);
            this.logHttpServer.createContext("/logs/summary", exchange -> {
                try {
                    String logFilePath = this.gossipReceiver.getSummaryLogFilePath();
                    String logContent = readLogFileByPath(logFilePath);
                    byte[] response = logContent.getBytes(java.nio.charset.StandardCharsets.UTF_8);

                    exchange.getResponseHeaders().add("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(200, response.length);

                    try (java.io.OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } catch (Exception e) {
                    String error = "Error reading summary log: " + e.getMessage();
                    byte[] errorBytes = error.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(500, errorBytes.length);
                    try (java.io.OutputStream os = exchange.getResponseBody()) {
                        os.write(errorBytes);
                    }
                }
            });

            this.logHttpServer.createContext("/logs/verbose", exchange -> {
                try {
                    String logFilePath = this.gossipReceiver.getVerboseLogFilePath();
                    String logContent = readLogFileByPath(logFilePath);
                    byte[] response = logContent.getBytes(java.nio.charset.StandardCharsets.UTF_8);

                    exchange.getResponseHeaders().add("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(200, response.length);

                    try (java.io.OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } catch (Exception e) {
                    String error = "Error reading verbose log: " + e.getMessage();
                    byte[] errorBytes = error.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(500, errorBytes.length);
                    try (java.io.OutputStream os = exchange.getResponseBody()) {
                        os.write(errorBytes);
                    }
                }
            });
            this.logHttpServer.setExecutor(null);
            this.logHttpServer.start();
            this.logger.info("Log HTTP server started on port " + (this.myPort + 5));
        } catch (IOException e) {
            this.logger.log(Level.WARNING, "Failed to start log HTTP server", e);
        }
    }
    @Override
    public boolean isPeerDead(long id){
        return this.failedServersIDs.contains(id);
    }
    @Override public boolean isPeerDead(InetSocketAddress address){
        return this.failedServersAdresses.contains(address);
    }

    @Override
    public void reportFailedPeer(long id){
        this.failedServersIDs.add(id);
        InetSocketAddress deadServer = new InetSocketAddress(this.peerIDtoAddress.get(id).getHostString(), this.peerIDtoAddress.get(id).getPort()+2);
        this.failedServersAdresses.add(deadServer);
    }
    @Override
    public void shutdown(){
        this.logger.info("Shutting down PeerServerImpl on port " + this.myPort);
        this.shutdown = true;
        if(this.serverThread != null && this.serverThread.isAlive()){
            if(this.serverThread instanceof RoundRobinLeader){
                ((RoundRobinLeader)this.serverThread).shutdown();
            }
            else if(this.serverThread instanceof JavaRunnerFollower){
                ((JavaRunnerFollower)this.serverThread).shutdown();
            }
            this.serverThread.interrupt();
            try {
                this.serverThread.join(1000);
            } catch (InterruptedException e) {
                this.logger.warning("Interrupted while joining serverThread");
            }
        }
        if(this.detectorThread != null && this.detectorThread.isAlive()){
            if(this.detectorThread instanceof LeaderFailureDetector){
                ((LeaderFailureDetector)this.detectorThread).shutdown();
            }
            else if(this.detectorThread instanceof NonLeaderFailureDetector){
                ((NonLeaderFailureDetector)this.detectorThread).shutdown();
            }
            this.detectorThread.interrupt();
        }
        if(this.tcpReceiver != null){
            try{
                this.tcpReceiver.shutdown();
            }
            catch (IOException e){
                this.logger.log(Level.WARNING, "Error shutting down TCP receiver", e);
            }
        }
        if(tcpReceiverThread != null && tcpReceiverThread.isAlive()){
            tcpReceiverThread.interrupt();
        }
        if(tcpSenderThread != null && tcpSenderThread.isAlive()){
            tcpSenderThread.interrupt();
        }
        if(this.tcpSender != null){
            try{
                this.tcpSender.shutdown();
            }
            catch (IOException e){
                this.logger.log(Level.WARNING, "Error shutting down TCP sender", e);
            }
        }
        if(this.udpReceiver != null){
            this.udpReceiver.shutdown();
        }
        if(this.udpSender != null){
            this.udpSender.shutdown();
        }
        if(this.udpHeartbeatSender != null){
            this.udpHeartbeatSender.shutdown();
        }
        if(this.udpHeartbeatReceiver != null){
            this.udpHeartbeatReceiver.shutdown();
        }
        if (gossipSender != null) {
            gossipSender.shutdown();
        }
        if (gossipReceiver != null) {
            gossipReceiver.shutdown();
        }
        if (this.logHttpServer != null) {
            this.logHttpServer.stop(0);
            this.logger.info("Log HTTP server stopped");
        }
        this.interrupt();
        this.logger.info("PeerServerImpl on port " + this.myPort + " shutdown complete");
    }
    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
        this.logger.log(Level.INFO, currentLeader==null ? "Current leader is set to null due to leader failure":"Current leader is " + this.currentLeader.toString());
        if(v==null){
            leaderAddress=null;
            return;
        }
        if(v.getProposedLeaderID()==this.getServerId()){
            this.leaderAddress = new InetSocketAddress(this.myAddress.getHostName(), this.myPort);
            this.logger.log(Level.INFO,currentLeader==null ? "Current leader address is set to null due to leader failure": "Current leader  address is " + this.leaderAddress.getHostString()+":"+this.leaderAddress.getPort());
        }
        else{
            this.leaderAddress = new InetSocketAddress(this.peerIDtoAddress.get(v.getProposedLeaderID()).getHostName(), this.peerIDtoAddress.get(v.getProposedLeaderID()).getPort());
            this.logger.log(Level.INFO, currentLeader==null ? "Current leader address is set to null due to leader failure":"Current leader  address is " + this.leaderAddress.getHostString()+":"+this.leaderAddress.getPort());
        }
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        if(type == Message.MessageType.ELECTION || type == Message.MessageType.GOSSIP){
            messageSender(type, messageContents, target, this.outgoingMessagesUDP, false);
        }
        else{
            messageSender(type, messageContents, target, this.outgoingMessagesTCP, true);
        }
    }
    private void messageSender(Message.MessageType type, byte[] messageContents, InetSocketAddress target, LinkedBlockingQueue<Message> outgoingMessages, boolean isTCP) throws IllegalArgumentException{
        Message msg;
        if(isTCP){
            msg = new Message(type,messageContents, this.myAddress.getHostString(), this.myPort+2, target.getHostString(), target.getPort()+2);
        }
        else{
            msg = new Message(type,messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        }
        outgoingMessages.add(msg);
    }
    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        if(type == Message.MessageType.ELECTION || type == Message.MessageType.GOSSIP){
            broadcastSender(type, messageContents, this.outgoingMessagesUDP);
        }
        else{
            broadcastSender(type, messageContents, this.outgoingMessagesTCP);
        }
    }

    private void broadcastSender(Message.MessageType type, byte[] messageContents, LinkedBlockingQueue<Message> outgoingMessages){
        for(Map.Entry<Long,InetSocketAddress> entry:this.peerIDtoAddress.entrySet()){
            if(entry.getKey().equals(this.id)){
                continue;
            }

            Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, entry.getValue().getHostString(), entry.getValue().getPort());
            outgoingMessages.add(msg);
        }
    }
    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        if(this.state == ServerState.OBSERVER){
            return;
        }
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    protected void setPeerEpoch(long peerEpoch) {
        this.peerEpoch = peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        if(this.peerIDtoAddress.containsKey(this.id) && this.state == ServerState.OBSERVER){
            return ((peerIDtoAddress.size() - this.numberOfObservers) /2)+1;
        }
        else if(!this.peerIDtoAddress.containsKey(this.id) && this.state == ServerState.OBSERVER){
            return ((peerIDtoAddress.size() - (this.numberOfObservers-1)) /2)+1;
        }
        else if(this.peerIDtoAddress.containsKey(this.id)){
            return ((peerIDtoAddress.size() - this.numberOfObservers) /2)+1;
        }
        else{
            return ((peerIDtoAddress.size() - this.numberOfObservers+1) /2)+1;
        }
//        int total = peerIDtoAddress.size() - this.numberOfObservers;
//        if(this.peerIDtoAddress.containsKey(this.id) && this.state == ServerState.OBSERVER){
//            total--;
//        }
//        else if(!this.peerIDtoAddress.containsKey(this.id) && this.state != ServerState.OBSERVER){
//            total++;
//        }
//        return (total/2) +1;
    }

    @Override
    public void run(){
        try{
            Thread.sleep(250);
        }
        catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
        try{
            if(this.state != ServerState.OBSERVER ||  this.id==this.gatewayID){
                this.tcpReceiver = new TCPMessageReceiver(this.incomingMessagesTCP, this.myPort+2);
                tcpReceiverThread = new Thread(this.tcpReceiver);
                tcpReceiverThread.setDaemon(true);
                tcpReceiverThread.start();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable To recieve message to " + this.myAddress,e);
        }
        //step 3: main server loop
        try{
            while (!this.shutdown && !Thread.currentThread().isInterrupted()){
                switch (getPeerState()){
                    case LOOKING:
                        //start leader election, set leader to the election winner
                        //might need to chnage this logic in stage 5 when multiple leader election can happen STAGE 5
                        LeaderElection leaderElection= new LeaderElection(this, this.incomingMessagesUDP,this.logger);
                        leaderElection.lookForLeader();
                        while(!this.completedWorkQueue.isEmpty()){
                            Message msg = this.completedWorkQueue.poll();
                            Message completedWork = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, msg.getMessageContents(), msg.getSenderHost(), msg.getSenderPort(), this.leaderAddress.getHostString(), this.leaderAddress.getPort(), msg.getRequestID());
                            this.outgoingMessagesTCP.add(completedWork);
                        }
                        if(this.serverThread!=null){
                            ((JavaRunnerFollower)this.serverThread).shutdown();//wotn be a leader becuase leader wont do another election if it crashes
                            this.serverThread = null;
                        }
                        break;
                    case OBSERVER:
                        Vote leader = this.currentLeader;
                        if(leader!=null && this.id==leader.getProposedLeaderID()){
                            this.currentLeader = new Vote(-1L,-1L);//stage 5 porb need to detect if leader is down here too
                            LeaderElection observerLeaderElection= new LeaderElection(this, this.incomingMessagesUDP, this.logger);
                            observerLeaderElection.lookForLeader();
                            this.logger.fine("The leader for this OBSERVER is " + this.currentLeader.getProposedLeaderID());
                        }
                        else if( leader==null){
                            if(tcpSender != null) {
                                tcpSender.shutdown();
                            }
                            this.currentLeader = new Vote(-1L,-1L);//stage 5 porb need to detect if leader is down here too
                            LeaderElection observerLeaderElection= new LeaderElection(this, this.incomingMessagesUDP, this.logger);
                            observerLeaderElection.lookForLeader();
                            this.logger.fine("The leader for this OBSERVER is now " + this.currentLeader.getProposedLeaderID());
                            ArrayList<Message> unsentMessages = new  ArrayList<>();
                            this.outgoingMessagesTCP.drainTo(unsentMessages);
                            for(Message msg : unsentMessages){
                                outgoingMessagesTCP.add(new Message(Message.MessageType.WORK, msg.getMessageContents(),msg.getSenderHost(),msg.getSenderPort(),this.leaderAddress.getHostString(), this.leaderAddress.getPort(), msg.getRequestID()));
                            }
                            tcpSender = null;
                        }
                        if(this.tcpSender ==null){
                            try {
                                this.logger.log(Level.INFO, "Messages client sent while gateway had no leader: "+outgoingMessagesTCP);
                                tcpSender = new TCPMessageSender(outgoingMessagesTCP,this.getUdpPort()+2,this.leaderAddress.getHostName(), this.leaderAddress.getPort()+2);
                            } catch (IOException e) {
                                this.logger.log(Level.SEVERE, "Unable to start sender that communicates to the leader, shutting down.",e);
                                this.shutdown();
                                return;
                            }
                            if(this.id == this.gatewayID){
                                this.tcpSenderThread = new  Thread(tcpSender);
                                this.tcpSenderThread.setDaemon(true);
                                this.tcpSenderThread.setName("TCPSender-on-gateway-sending-to-leader");
                                this.tcpSenderThread.start();
                            }
                        }
                        if(this.detectorThread==null){
                            this.detectorThread = new NonLeaderFailureDetector(this.heartbeatVectorClock, this.peerIDtoAddress, this, this.id);
                            this.detectorThread.start();
                        }
                        break;
                    case LEADING:
                        if((this.serverThread == null || this.serverThread instanceof JavaRunnerFollower)){

//                            if(this.observersSet.size()!=numberOfObservers){
//                                throw new IllegalStateException("Observer set size does not match number of observers");
//                            }
                            if(this.serverThread != null){
                                ((JavaRunnerFollower)this.serverThread).shutdown();
                            }
                            this.serverThread= new RoundRobinLeader(peerIDtoAddress, this.incomingMessagesTCP, this.myAddress, this.myPort+2, this.gatewayID, this.failedServersAdresses);
                            this.serverThread.setName("RoundRobinLeader-"+this.myPort+2);
                            this.serverThread.start();
                            this.logger.fine("RoundRobinLeader thread started");
                        }
                        if((this.detectorThread==null || this.detectorThread instanceof NonLeaderFailureDetector)){
                            if(this.detectorThread != null){
                                ((NonLeaderFailureDetector)this.detectorThread).shutdown();
                                this.logger.fine(this.id+": switching from FOLLOWER to LEADER");
                                System.out.println(this.id+": switching from FOLLOWER to LEADER");
                            }
                            this.detectorThread = new LeaderFailureDetector(this.heartbeatVectorClock,this.peerIDtoAddress,this, this.id);
                            this.detectorThread.start();
                            this.logger.fine("LeaderFailureDetector thread started");
                        }

                        break;
                    case FOLLOWING:
                        if(this.serverThread == null || this.serverThread instanceof RoundRobinLeader){
                            if(this.serverThread != null){
                                ((RoundRobinLeader)this.serverThread).shutdown();
                            }
                            this.serverThread = new JavaRunnerFollower(this.outgoingMessagesTCP, this.incomingMessagesTCP, this.myAddress.getHostString(), this.myPort + 2, this, this.completedWorkQueue);
                            this.serverThread.start();
                            this.logger.fine("JavaRunnerFollower thread started");
                        }
                        if((this.detectorThread==null || this.detectorThread instanceof LeaderFailureDetector)){
                            if(this.detectorThread != null){
                                ((LeaderFailureDetector)this.detectorThread).shutdown();
                            }
                            this.detectorThread = new NonLeaderFailureDetector(this.heartbeatVectorClock,this.peerIDtoAddress,this, this.id);
                            this.detectorThread.start();
                            this.logger.fine("FollowerFailureDetector thread started");
                        }
                        break;

                }
            }
        }
        catch (Exception e) {
            //code...
            this.logger.log(Level.SEVERE,"Exception occurred", e);
        }
        this.logger.info("PeerServerImpl thread on port " + this.myPort + " exiting run() method");
    }
    private String readLogFileByPath(String filePath) {
        try {
            Path logPath = Paths.get(filePath);
            if (java.nio.file.Files.exists(logPath)) {
                return java.nio.file.Files.readString(logPath);
            } else {
                return "Log file not found: " + filePath;
            }
        } catch (Exception e) {
            return "Error reading log file: " + e.getMessage();
        }
    }

}
