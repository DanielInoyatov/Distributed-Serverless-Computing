package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class  PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final InetSocketAddress myAddress;
    protected final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessagesUDP;
    private final LinkedBlockingQueue<Message> incomingMessagesUDP;
    protected final LinkedBlockingQueue<Message> incomingMessagesTCP;//made this protected so the gatewaypeerserverimpl can access it, is that an issue?
    protected final LinkedBlockingQueue<Message> outgoingMessagesTCP;
    protected Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long,InetSocketAddress> peerIDtoAddress;//based of the demo code, the peerserver will never have its owm key value in this map
    protected Logger logger;
    private Thread serverThread;
    private UDPMessageSender udpSender;
    private UDPMessageReceiver udpReceiver;
    protected TCPMessageSender tcpSender;
    protected TCPMessageReceiver tcpReceiver;
    protected long gatewayID;
    private int numberOfObservers;
    protected InetSocketAddress leaderAddress;
    private Thread tcpReceiverThread;
    private Thread tcpSenderThread;
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
        this.id = serverID;
        this.peerEpoch = peerEpoch;
        this.currentLeader = new Vote(this.id, this.peerEpoch);
        this.peerIDtoAddress = peerIDtoAddress;
        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;
        //step 1: create and run thread that sends broadcast messages
        this.udpSender = new UDPMessageSender(this.outgoingMessagesUDP, this.myPort);
        this.udpSender.start();
        //step 2: create and run thread that listens for messages sent to this server
        try {
            this.udpReceiver= new UDPMessageReceiver(this.incomingMessagesUDP, this.myAddress, this.myPort, this);
            this.udpReceiver.start();
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
        setName("PeerServerImpl-" + this.myPort);
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
        this.interrupt();
        this.logger.info("PeerServerImpl on port " + this.myPort + " shutdown complete");
    }
    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
        if(v.getProposedLeaderID()==this.getServerId()){
            this.leaderAddress = new InetSocketAddress(this.myAddress.getHostName(), this.myPort);
        }
        else{
            this.leaderAddress = new InetSocketAddress(this.peerIDtoAddress.get(v.getProposedLeaderID()).getHostName(), this.peerIDtoAddress.get(v.getProposedLeaderID()).getPort());

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
                        LeaderElection leaderElection= new LeaderElection(this, this.incomingMessagesUDP, this.logger);
                        leaderElection.lookForLeader();
                        break;
                    case OBSERVER:
                        if(this.id==this.currentLeader.getProposedLeaderID()){
                            this.currentLeader = new Vote(-1L,-1L);//stage 5 porb need to detect if leader is down here too
                            LeaderElection observerLeaderElection= new LeaderElection(this, this.incomingMessagesUDP, this.logger);
                            observerLeaderElection.lookForLeader();
                            this.logger.fine("The leader for this OBSERVER is " + this.currentLeader.getProposedLeaderID());
                        }
                        if(this.tcpSender ==null){
                            try {
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

                        break;
                    case LEADING:
                        if((this.serverThread == null || this.serverThread instanceof JavaRunnerFollower)){

//                            if(this.observersSet.size()!=numberOfObservers){
//                                throw new IllegalStateException("Observer set size does not match number of observers");
//                            }
                            if(this.serverThread != null){
                                ((JavaRunnerFollower)this.serverThread).shutdown();
                            }
                            this.serverThread= new RoundRobinLeader(peerIDtoAddress, this.incomingMessagesTCP, this.myAddress, this.myPort+2, this.gatewayID);
                            this.serverThread.setName("RoundRobinLeader-"+this.myPort+2);
                            this.serverThread.start();
                            this.logger.fine("RoundRobinLeader thread started");
                        }


                        break;
                    case FOLLOWING:
                        if(this.serverThread == null || this.serverThread instanceof RoundRobinLeader){
                            if(this.serverThread != null){
                                ((RoundRobinLeader)this.serverThread).shutdown();
                            }
                            this.serverThread = new JavaRunnerFollower(this.outgoingMessagesTCP, this.incomingMessagesTCP, this.myAddress.getHostString(), this.myPort + 2, this.peerIDtoAddress.get(this.getCurrentLeader().getProposedLeaderID()).getHostName(),this.peerIDtoAddress.get(this.getCurrentLeader().getProposedLeaderID()).getPort()+2);
                            this.serverThread.setName("JavaRunnerFollower-"+(this.myPort+2));
                            this.serverThread.start();
                            this.logger.fine("JavaRunnerFollower thread started");
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

}
