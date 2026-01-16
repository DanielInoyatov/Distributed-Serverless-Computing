package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LeaderFailureDetector extends Thread implements LoggingServer {
    private ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private PeerServerImpl  peerServer;
    private long id;
    private final Logger logger;

    public LeaderFailureDetector(ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock, Map<Long, InetSocketAddress> peerIDtoAddress, PeerServerImpl peerServer, Long id) throws IOException {
        this.heartbeatVectorClock = heartbeatVectorClock;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerServer = peerServer;
        this.id = id;
        setName("Thread-LeaderFailureDetector-At-"+this.peerServer.getUdpPort()+1);
        setDaemon(true);
        this.logger = initializeLogging(LeaderFailureDetector.class.getCanonicalName() + "-on-port-" +this.peerServer.getUdpPort()+1);
        this.logger.setUseParentHandlers(false);
    }

    @Override
    public void run() {
        while(!this.isInterrupted()){
            Set<Long> cleanup = new HashSet<Long>();
            for(Heartbeat heartbeat : this.heartbeatVectorClock.values()){
                if(heartbeat.getCurrentTime()+7500 < System.currentTimeMillis() && this.id!=heartbeat.getPeerId() && !this.peerServer.isPeerDead(heartbeat.getPeerId())){//30_000
                    this.peerServer.reportFailedPeer(heartbeat.getPeerId());
                    this.logger.log(Level.WARNING, this.id+": no heartbeat from server "+heartbeat.getPeerId()+" - SERVER FAILED\n");
                    System.out.println(this.id+": no heartbeat from server "+heartbeat.getPeerId()+" - SERVER FAILED");
                    this.logger.log(Level.WARNING, "Dead Servers:"+this.peerServer.failedServersIDs+"\n");
                }
                else if(heartbeat.getCurrentTime()+(7500 * 2) < System.currentTimeMillis()  && this.id!=heartbeat.getPeerId()){//change to 30_000
                    this.peerIDtoAddress.remove(heartbeat.getPeerId());
                    cleanup.add(heartbeat.getPeerId());
                }
            }
            for (long id : cleanup) {
                heartbeatVectorClock.remove(id);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                this.interrupt();
            }
        }
    }

    public void shutdown(){
        this.interrupt();
    }
}
