package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipReceiver extends Thread implements LoggingServer {
    private ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock;
    private LinkedBlockingQueue<Message> incomingMessagesMessagesHeartbeatUDP;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private PeerServerImpl peerServer;
    private long id;
    private final Logger verboseLogger;
    private final Logger summaryLogger;
    private final String verboseLogFilePath;
    private final String summaryLogFilePath;
    protected GossipReceiver(ConcurrentHashMap<Long, Heartbeat> heartbeatVectorClock, LinkedBlockingQueue<Message> incomingMessagesMessagesHeartbeatUDP, Map<Long, InetSocketAddress> peerIDtoAddress, PeerServerImpl peerServer, Long id) throws IOException {
        this.heartbeatVectorClock = heartbeatVectorClock;
        this.incomingMessagesMessagesHeartbeatUDP = incomingMessagesMessagesHeartbeatUDP;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerServer = peerServer;
        this.id = id;
        setName("Thread-GossipReceiver-At-"+this.peerServer.getUdpPort()+1);
        setDaemon(true);
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        String dirName = "logs-" + suffix;

        String verbosePreface = "VerboseGossipReceiver-on-port-" + (this.peerServer.getUdpPort()+1);
        String summaryPreface = "SummaryGossipReceiver-on-port-" + (this.peerServer.getUdpPort()+1);

        this.verboseLogger = initializeLogging(verbosePreface);
        this.verboseLogger.setUseParentHandlers(false);
        this.verboseLogFilePath = dirName + File.separator + verbosePreface + "-Log.txt";

        this.summaryLogger = initializeLogging(summaryPreface);
        this.summaryLogger.setUseParentHandlers(false);
        this.summaryLogFilePath = dirName + File.separator + summaryPreface + "-Log.txt";
    }

    @Override
    public void run() {
        while(!this.isInterrupted()) {
            try {
                Message message = incomingMessagesMessagesHeartbeatUDP.take();
                InetSocketAddress sender = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
                String[] heartBeatStrings =  new String(message.getMessageContents()).split("=");
                StringBuilder sb = new StringBuilder();
                long senderID = Long.parseLong(heartBeatStrings[0]);
                String[] heartBeatStringsTemp = new String[heartBeatStrings.length-1];
                for (int i = 1; i < heartBeatStrings.length; i++) {
                    sb.append(heartBeatStrings[i]).append('\n');
                    heartBeatStringsTemp[i-1] = heartBeatStrings[i];
                }
                heartBeatStrings=heartBeatStringsTemp;
                String result = sb.toString();
                this.verboseLogger.log(Level.INFO, this.id+":Received heartbeat vector clock from "+ message.getSenderHost() + ":" + message.getSenderPort()+"at time "+ System.currentTimeMillis()+". Contents are:\n"+result+"\n");
                for(String heartBeatString : heartBeatStrings){
                    String[] parts = heartBeatString.split(", ");
                    long peerId = Long.parseLong(parts[0].split(": ")[1]);
                    long sequence = Long.parseLong(parts[1].split(": ")[1]);
                    if(!this.peerServer.isPeerDead(sender)){
                        if(heartbeatVectorClock.containsKey(peerId) && heartbeatVectorClock.get(peerId).getHeartbeat()<sequence){
                            heartbeatVectorClock.get(peerId).setHeartbeat(sequence);
                            this.summaryLogger.log(Level.INFO, this.id+": updated "+ peerId+"'s heartbeat sequence to "+heartbeatVectorClock.get(peerId).getHeartbeat() + " based on message from " + senderID+ " at node time " + heartbeatVectorClock.get(peerId).getCurrentTime()+"\n");
                        }
                    }
                }
            } catch (InterruptedException e) {
                this.interrupt();
            }

        }
    }
    protected String getVerboseLogFilePath() {
        return verboseLogFilePath;
    }

    protected String getSummaryLogFilePath() {
        return summaryLogFilePath;
    }
    protected void shutdown(){
        this.interrupt();
    }
}
