package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayPeerServerImpl extends PeerServerImpl{

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers){
        super(myPort, peerEpoch, id, peerIDtoAddress, gatewayID, numberOfObservers);
        this.setPeerState(ServerState.OBSERVER);
        setName("GatewayPeerServerImpl-" + myPort);
    }

    @Override
    public void shutdown(){
        super.shutdown();
    }

    protected LinkedBlockingQueue<Message> getIncomingMessagesTCP(){
        return incomingMessagesTCP;
    }
    protected LinkedBlockingQueue<Message> getOutgoingMessagesTCP(){
        return outgoingMessagesTCP;
    }

}
