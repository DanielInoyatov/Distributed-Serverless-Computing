package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.PeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NonLeaderFailureDetector extends Thread implements LoggingServer {
    private ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private final long id;
    private PeerServerImpl peerServerImpl;
    private final Logger logger;

    public NonLeaderFailureDetector(ConcurrentHashMap<Long,Heartbeat> heartbeatVectorClock, Map<Long, InetSocketAddress> peerIDtoAddress, PeerServerImpl peerServerImpl, long id) throws IOException {
        this.heartbeatVectorClock = heartbeatVectorClock;
        this.peerIDtoAddress = peerIDtoAddress;
        this.id = id;
        this.peerServerImpl = peerServerImpl;
        if(this.id == this.peerServerImpl.gatewayID){
            setName("Thread-GatewayFailureDetector-At-"+(this.peerServerImpl.getUdpPort() +1));
            this.logger = initializeLogging("GatewayFailureDetector" + "-on-port-" +this.peerServerImpl.getUdpPort()+1);
            this.logger.setUseParentHandlers(false);
        }
        else{
            setName("Thread-FollowerFailureDetector-At-"+this.peerServerImpl.getUdpPort()+1);
            this.logger = initializeLogging("FollowerFailureDetector" + "-on-port-" +this.peerServerImpl.getUdpPort()+1);
            this.logger.setUseParentHandlers(false);
        }
        setDaemon(true);
    }

    @Override
    public void run() {
        while(!this.isInterrupted()){
            Set<Long> cleanup = new HashSet<>();
            for(Heartbeat heartbeat : this.heartbeatVectorClock.values()){
                if(heartbeat.getPeerId()!= this.id &&  heartbeat.getCurrentTime()+7500 < System.currentTimeMillis() && !this.peerServerImpl.isPeerDead(heartbeat.getPeerId())){// change to 30_000 once done
                    boolean leaderDied = false;
                    InetSocketAddress failedAddress = peerIDtoAddress.get(heartbeat.getPeerId());
                    if(failedAddress!=null && failedAddress.equals(peerServerImpl.leaderAddress)){
                        leaderDied =true;
                        this.logger.log(Level.WARNING, this.id+": no heartbeat from server "+heartbeat.getPeerId()+" - SERVER FAILED");
                        this.logger.log(Level.INFO, "Failed server was a LEADER\n");
                        System.out.println(this.id+": no heartbeat from server "+heartbeat.getPeerId()+" - SERVER FAILED");
                        this.peerServerImpl.setPeerEpoch(this.peerServerImpl.getPeerEpoch()+1);
                        this.peerServerImpl.setPeerState(PeerServer.ServerState.LOOKING);
                        try {
                            this.peerServerImpl.setCurrentLeader(null);
                        } catch (IOException e) {
                            this.interrupt();
                        }
                    }
                    if(!leaderDied){
                        this.logger.log(Level.WARNING, this.id+": no heartbeat from server "+heartbeat.getPeerId()+" - SERVER FAILED");
                        this.logger.log(Level.INFO, "Failed server was a FOLLOWER\n");
                        System.out.println(this.id+": no heartbeat from server "+heartbeat.getPeerId()+" - SERVER FAILED");
                    }
                    this.peerServerImpl.reportFailedPeer(heartbeat.getPeerId());
                    this.logger.log(Level.WARNING, "Dead Servers:"+this.peerServerImpl.failedServersIDs+"\n");
                }
                else if(heartbeat.getCurrentTime()+(7500 * 2) < System.currentTimeMillis() && this.id!=heartbeat.getPeerId()){// change to 30_000 once done
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
