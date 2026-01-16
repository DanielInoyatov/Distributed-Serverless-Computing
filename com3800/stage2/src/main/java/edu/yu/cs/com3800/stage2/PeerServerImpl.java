package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class  PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long,InetSocketAddress> peerIDtoAddress;
    private Logger logger;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress){
        //code here...
        this.myPort = myPort;
        this.myAddress =  new InetSocketAddress("localhost", this.myPort);
        this.state = ServerState.LOOKING;
        this.shutdown = false;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.id = id;
        this.peerEpoch = peerEpoch;
        this.currentLeader = new Vote(this.id, this.peerEpoch);
        this.peerIDtoAddress = peerIDtoAddress;
        try{
            this.logger = initializeLogging(PeerServerImpl.class.getCanonicalName() + "-on-port-" + this.myPort);
            this.logger.setUseParentHandlers(false);

        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown(){
        this.shutdown = true;
        if(this.receiverWorker != null){
            this.receiverWorker.shutdown();
        }
        if(this.senderWorker != null){
            this.senderWorker.shutdown();
        }
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        Message msg = new Message(type,messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        this.outgoingMessages.add(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(Map.Entry<Long,InetSocketAddress> entry:this.peerIDtoAddress.entrySet()){
            if(entry.getKey().equals(this.id)){
                continue;
            }
            Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, entry.getValue().getHostString(), entry.getValue().getPort());
            this.outgoingMessages.add(msg);
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
        int total = peerIDtoAddress.size();
        if(!this.peerIDtoAddress.containsKey(this.id)){
            total++;
        }
        return (total/2) +1;
    }

    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        this.senderWorker.start();
        //step 2: create and run thread that listens for messages sent to this server
        try {
            this.receiverWorker= new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
            this.receiverWorker.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //step 3: main server loop
        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        //start leader election, set leader to the election winner
                        LeaderElection leaderElection= new LeaderElection(this, this.incomingMessages, this.logger);
                        leaderElection.lookForLeader();
                        break;
                }
            }
        }
        catch (Exception e) {
            //code...
            this.logger.log(Level.SEVERE,"Failed to Elect Leader", e);
        }
    }

}
