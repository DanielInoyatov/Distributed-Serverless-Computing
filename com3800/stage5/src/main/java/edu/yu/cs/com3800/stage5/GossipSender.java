package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.UDPMessageSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipSender extends Thread implements LoggingServer {
    private ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock;
    private LinkedBlockingQueue<Message> outgoingMessagesHeartbeatUDP;;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private PeerServerImpl  peerServerImpl;
    private long id;
    private final Logger logger;
    private ArrayList<Long> peerIds;
    protected GossipSender(ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock, LinkedBlockingQueue<Message> outgoingMessagesHeartbeatUDP, Map<Long, InetSocketAddress> peerIDtoAddress, PeerServerImpl peerServer, long id) throws IOException {
        this.heartbeatVectorClock = heartbeatVectorClock;
        this.outgoingMessagesHeartbeatUDP = outgoingMessagesHeartbeatUDP;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerServerImpl = peerServer;
        this.id = id;
        this.peerIds = new ArrayList<>(peerIDtoAddress.keySet());
        setName("Thread-GossipSender-At-"+(this.peerServerImpl.getUdpPort()+1));
        setDaemon(true);
        this.logger = initializeLogging(GossipSender.class.getCanonicalName() + "-on-port-" +(this.peerServerImpl.getUdpPort()+1));
        this.logger.setUseParentHandlers(false);
    }

    @Override
    public void run() {
        while (!this.isInterrupted()){
            try {
                Thread.sleep(750);//change to 3000 once finished
                Long randomPeer = peerIds.get(ThreadLocalRandom.current().nextInt(peerIds.size()));
                if(randomPeer == id || this.peerServerImpl.isPeerDead(randomPeer) || heartbeatVectorClock.get(randomPeer)==null){
                    continue;
                }
                heartbeatVectorClock.get(id).incrementHeartbeat();
                StringBuilder sb = new StringBuilder();
                sb.append(this.id+"=");
                for (Heartbeat heartbeat : heartbeatVectorClock.values()) {
                    sb.append(heartbeat).append('=');
                }
                String messageContent = sb.toString();
                this.logger.log(Level.INFO, this.id+": GOSSIPING to "+randomPeer);
                Message gossipMessage = new Message(Message.MessageType.GOSSIP, messageContent.getBytes(StandardCharsets.UTF_8), this.peerServerImpl.getAddress().getHostString(), peerServerImpl.getUdpPort()+1, peerIDtoAddress.get(randomPeer).getHostString(), (peerIDtoAddress.get(randomPeer).getPort()+1));
                this.outgoingMessagesHeartbeatUDP.put(gossipMessage);

            } catch (InterruptedException e) {
                this.interrupt();
            }
        }
    }

    public void shutdown(){
        this.interrupt();
    }
}