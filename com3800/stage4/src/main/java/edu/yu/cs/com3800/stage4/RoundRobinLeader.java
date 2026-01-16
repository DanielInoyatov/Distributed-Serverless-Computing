package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
    protected RoundRobinLeader(Map<Long, InetSocketAddress> peerIDtoAddress, LinkedBlockingQueue<Message>  incomingMessagesTCP, InetSocketAddress myAddress, int myPort, long gatewayId) {
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

    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            if(serverToAssignTo <0){//accounting for overflows, which wont happen but should still be taken into consideration
                serverToAssignTo = 0;
            }

            try {
                InetSocketAddress nextServerAddress = this.servers.get((int)serverToAssignTo%(servers.size()));
                Message message = incomingMessagesTCP.take();
                if(message.getMessageType()== Message.MessageType.WORK){
                    //keeping the client in the sender slot so the worker knows who the request is from

                    Message outgoingMessage = new Message(message.getMessageType(), message.getMessageContents(),  this.myAddress, this.myPort, nextServerAddress.getHostString(), nextServerAddress.getPort(), message.getRequestID());
                    requestIDsTOClientsAddress.put(message.getRequestID(),  new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
                    this.sendersQueueMap.get(nextServerAddress).add(outgoingMessage);
                    //this.outgoingMessagesUDP.put(outgoingMessage);
                    this.logger.fine("Received work from "+ requestIDsTOClientsAddress.get(message.getRequestID()) +". The request ID  is " + message.getRequestID()+".\nSending work to WORKER " + serverToAssignTo%(servers.size())+ " who lives at " +nextServerAddress.getHostString() + ":" +nextServerAddress.getPort());
                    String receivedCode = new String(message.getMessageContents(), StandardCharsets.UTF_8);
                    this.logger.info("===WORKER RECEIVED CODE===\n" + receivedCode + "\n===END===\n");
                    this.serverToAssignTo++;
                }
                else if (message.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                    if(message.getRequestID()==-1){
                        this.logger.log(Level.INFO, "Received back work with -1 requestID");
                    }
                    else{
                        //the message sender host and port is the clients info, sending back to the client
                        InetSocketAddress incomingAddress = requestIDsTOClientsAddress.get(message.getRequestID());
                        if(incomingAddress==null){
                            continue;
                        }

                        String clientHostString = incomingAddress.getHostString();
                        int clientPort = incomingAddress.getPort();
                        Message outgoingMessage = new Message(message.getMessageType(), message.getMessageContents(),this.myAddress, this.myPort, clientHostString,clientPort, message.getRequestID());
                        this.sendersQueueMap.get(gatewayAddress).add(outgoingMessage);
                        this.logger.fine("Recieved COMPLETED_WORK with requestID " + message.getRequestID() + " from WORKER  who lives at " + message.getSenderHost()+":"+message.getSenderPort()+"\nSending COMPLETED_WORK back to client at "+ requestIDsTOClientsAddress.get(message.getRequestID())+"\n");
                        this.requestIDsTOClientsAddress.remove(message.getRequestID());
                    }

                }
                else{
                    boolean done = false;
                    while(!done){
                        done = this.incomingMessagesTCP.offer(message);
                    }
                    this.logger.warning("Message recieved was not of type WORK or COMPLETED_WORK. Added back to the queue\n");
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



