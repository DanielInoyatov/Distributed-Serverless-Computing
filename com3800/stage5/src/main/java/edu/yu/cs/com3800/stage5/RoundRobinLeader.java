package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

//create a tcprecver and incoming tcp queue
//create a map of inetaddress to lb queue for outgoing
//create a thread pool, where each thread 1) takes from incoming 2) round robin decides who will proccess work, use the tcp sender to send work
public class RoundRobinLeader extends Thread implements LoggingServer {
    private ArrayList<InetSocketAddress> servers;
    private final LinkedBlockingQueue<Message> incomingMessagesTCP;
    private final String myAddress;
    private final int myPort;
    private long serverToAssignTo = 0;
    private final Logger logger;
    private final HashMap<Long, InetSocketAddress> requestIDsTOClientsAddress = new HashMap<>();
    private final ConcurrentHashMap<InetSocketAddress, LinkedBlockingQueue<Message>> sendersQueueMap = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName("Thread-in-round-robin-leader-executor");
                    return t;
                }
            }
    );
    private InetSocketAddress gatewayAddress;
    private HashMap<InetSocketAddress, HashMap<Long, Message>> workSentMap = new HashMap<>();//used for resending work sent to a dead follower
    private Set<InetSocketAddress> failedServers;
    private volatile int numFailedServers;
    private final HashMap<Long, String> requestIDsToResults = new HashMap<>();
    protected RoundRobinLeader(Map<Long, InetSocketAddress> peerIDtoAddress, LinkedBlockingQueue<Message>  incomingMessagesTCP, InetSocketAddress myAddress, int myPort, long gatewayId, Set<InetSocketAddress> failedServers) {
        this.myAddress = myAddress.getHostString();
        this.myPort = myPort;
        try{
            this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + this.myPort);
            this.logger.setUseParentHandlers(false);
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        this.incomingMessagesTCP = incomingMessagesTCP;
        try{
            gatewayAddress = new InetSocketAddress(peerIDtoAddress.get(gatewayId).getHostName(), peerIDtoAddress.get(gatewayId).getPort()+2);
            this.sendersQueueMap.put(gatewayAddress, new LinkedBlockingQueue<>());
            this.executorService.submit(new TCPMessageSender(this.sendersQueueMap.get(gatewayAddress),this.myPort, gatewayAddress.getHostName(), gatewayAddress.getPort()));
        }
        catch (IOException e){
            this.logger.log(Level.SEVERE, "Error sending to gateway: " + gatewayId, e);
            throw new RuntimeException(e);
        }
        servers = new ArrayList<>();
        for(InetSocketAddress server: peerIDtoAddress.values()){
            InetSocketAddress tcpServer = new InetSocketAddress(server.getHostName(), server.getPort()+2);
            if(!tcpServer.equals(gatewayAddress)){
                servers.add(tcpServer);
            }
        }
        setName("RoundRobinLeader-port-" + this.myPort);
        setDaemon(true);
        //this.observerSet = observerSet;
        ArrayList<InetSocketAddress> goodServers = new ArrayList<>();
        for(InetSocketAddress server : servers){
            //if(!observerSet.contains(server)){
                try{
                    LinkedBlockingQueue <Message> queue = new LinkedBlockingQueue<>();
                    TCPMessageSender sender = new TCPMessageSender(queue,this.myPort, server.getHostString(), server.getPort());
                    this.executorService.submit(sender);
                    this.sendersQueueMap.put(server, queue);
                    goodServers.add(server);
                }
                catch (IOException e){
                    this.logger.log(Level.WARNING, "Error creating TCPMessageSender for reciver: " + server, e);
                    this.sendersQueueMap.remove(server);
                }
            //}

        }
        servers = goodServers;
        this.failedServers = failedServers;
        this.numFailedServers = this.failedServers.size();
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            if(serverToAssignTo <0){//accounting for overflows, which wont happen but should still be taken into consideration
                serverToAssignTo = 0;
            }

            try {

                Message message = incomingMessagesTCP.take();
                InetSocketAddress senderAddress = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
                if(message.getMessageType()== Message.MessageType.WORK){
                    if(this.requestIDsToResults.containsKey(message.getRequestID())){
                        String cachedResult = this.requestIDsToResults.get(message.getRequestID());
                        this.sendersQueueMap.get(gatewayAddress).add(new Message(Message.MessageType.COMPLETED_WORK, cachedResult.getBytes(StandardCharsets.UTF_8), this.myAddress, this.myPort, gatewayAddress.getHostString(), gatewayAddress.getPort(), message.getRequestID()));
                        this.requestIDsToResults.remove(message.getRequestID());
                    }
                    else{
                        InetSocketAddress nextServerAddress = this.servers.get((int)serverToAssignTo%(servers.size()));
                        while(failedServers.contains(nextServerAddress)){
                            this.servers.remove(nextServerAddress);
                            nextServerAddress = this.servers.get((int)serverToAssignTo%(servers.size()));
                        }//this loop makes sure we dont assign work to a failed server

                        //keeping the client in the sender slot so the worker knows who the request is from
                        Message outgoingMessage = new Message(message.getMessageType(), message.getMessageContents(),  this.myAddress, this.myPort, nextServerAddress.getHostString(), nextServerAddress.getPort(), message.getRequestID());
                        requestIDsTOClientsAddress.put(message.getRequestID(), senderAddress);
                        this.sendersQueueMap.get(nextServerAddress).add(outgoingMessage);
                        //this.outgoingMessagesUDP.put(outgoingMessage);
                        this.logger.fine("Received work from "+ requestIDsTOClientsAddress.get(message.getRequestID()) +". The request ID  is " + message.getRequestID()+".\nSending work to WORKER " + serverToAssignTo%(servers.size())+ " who lives at " +nextServerAddress.getHostString() + ":" +nextServerAddress.getPort());
                        String receivedCode = new String(message.getMessageContents(), StandardCharsets.UTF_8);
                        this.logger.info("===WORKER RECEIVED CODE===\n" + receivedCode + "\n===END===\n");
                        workSentMap.computeIfAbsent(nextServerAddress, v -> new HashMap<>())
                                .put(message.getRequestID(),outgoingMessage);
                        this.serverToAssignTo++;
                    }
                }
                else if (message.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK && !failedServers.contains(senderAddress)) {
                    this.requestIDsToResults.put(message.getRequestID(), new String(message.getMessageContents()));
                } else if (message.getMessageType() == Message.MessageType.COMPLETED_WORK && !failedServers.contains(senderAddress)) {
                    if(message.getRequestID()==-1){
                        this.logger.log(Level.INFO, "Received back work with -1 requestID");
                    }
                    else{
                        HashMap<Long, Message> workerWork = workSentMap.get(senderAddress);
                        if(workerWork != null) {
                            workerWork.remove(message.getRequestID());
                        }

                        InetSocketAddress incomingAddress = requestIDsTOClientsAddress.get(message.getRequestID());

                        Message outgoingMessage = new Message(message.getMessageType(), message.getMessageContents(), this.myAddress, this.myPort, gatewayAddress.getHostString(), gatewayAddress.getPort(), message.getRequestID());
                        this.sendersQueueMap.get(gatewayAddress).add(outgoingMessage);
                        this.logger.fine("Received COMPLETED_WORK with requestID " + message.getRequestID()+"\n");

                        if(incomingAddress != null) {
                            this.requestIDsTOClientsAddress.remove(message.getRequestID());
                        }
                    }
                }
                else if (failedServers.contains(senderAddress)) {//reassigning work from a dead follower to other followers
                    HashMap<Long, Message> resendMessages = workSentMap.remove(senderAddress);
                    for(Message msg:  resendMessages.values()){
                        incomingMessagesTCP.put(msg);
                    }
                }
                else{
                    this.incomingMessagesTCP.put(message);
                    this.logger.warning("Message recieved was not of type WORK or COMPLETED_WORK. Added back to the queue\n");
                }
                if(this.numFailedServers!=this.failedServers.size()){
                    for(InetSocketAddress failedAddress: failedServers){
                        if(workSentMap.containsKey(failedAddress)){
                            HashMap<Long, Message> resendMessages = workSentMap.remove(failedAddress);
                            if(resendMessages != null && !resendMessages.isEmpty()) {
                                for(Message msg: resendMessages.values()){
                                    incomingMessagesTCP.put(msg);
                                }
                            }
                        }
                    }
                    this.numFailedServers=failedServers.size();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                this.executorService.shutdownNow();
                this.logger.log(Level.SEVERE,"Thread interrupted. Stopping RoundRobinLeader.");
            }

        }
    }

    public void shutdown(){
        this.interrupt();
        this.executorService.shutdownNow();
    }
}



