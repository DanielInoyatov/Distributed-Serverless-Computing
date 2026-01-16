package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class GatewayPeerServerImpl extends PeerServerImpl {

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        super(myPort, peerEpoch, id, peerIDtoAddress, gatewayID, numberOfObservers);
        this.setPeerState(ServerState.OBSERVER);
        try {
            this.setCurrentLeader(null);
        } 
        catch (Exception e) {
        }
        setName("GatewayPeerServerImpl-" + myPort);
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    protected LinkedBlockingQueue<Message> getIncomingMessagesTCP() {
        return incomingMessagesTCP;
    }
    
    protected LinkedBlockingQueue<Message> getOutgoingMessagesTCP() {
        return outgoingMessagesTCP;
    }
}