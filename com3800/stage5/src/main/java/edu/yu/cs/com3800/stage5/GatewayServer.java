package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer extends Thread implements LoggingServer {
    /*
    Must create an HTTPServer on whatever port was passed in the constructor
    Needs to know who the leader is (probaly thru the gateway peer which is the observer)
    Is a peer in the cluster as an observer - meaning no vote in the elecetion, but knows who the leader is
    Must have a cache - thread safe map
        if cache hit, add http header Cached-Response and true
        else false
     */
    private Logger logger;
    private int httpPort;
    private final GatewayPeerServerImpl  gatewayPeerServerImpl;
    private final HttpServer httpServer;
    private final ConcurrentHashMap<Integer, String> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, LinkedBlockingQueue<String>> requestIdsToResults = new ConcurrentHashMap<>();
    private final AtomicLong requestID = new AtomicLong();
    private Thread dispatcher;
    private final HttpHandler statusHandler = new HttpHandler() {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                exchange.getResponseBody().close();
                return;
            }

            Vote leader = gatewayPeerServerImpl.getCurrentLeader();
            StringBuilder response = new StringBuilder();

            if (leader == null || leader.getProposedLeaderID() < 0) {
                response.append("NO_LEADER");
            }
            else {
                response.append("LEADER:").append(leader.getProposedLeaderID()).append("\n");

                Map<Long, InetSocketAddress> peers = gatewayPeerServerImpl.peerIDtoAddress;
                for (Map.Entry<Long, InetSocketAddress> entry : peers.entrySet()) {
                    Long peerId = entry.getKey();
                    if (gatewayPeerServerImpl.isPeerDead(peerId)) {
                        continue;
                    }

                    String role;
                    if (peerId.equals(leader.getProposedLeaderID())) {
                        role = "LEADER";
                    } else if (peerId.equals(gatewayPeerServerImpl.getServerId())) {
                        role = "OBSERVER";
                    } else {
                        role = "FOLLOWER";
                    }
                    response.append(peerId).append(":").append(role).append("\n");
                }
            }

            byte[] responseBytes = response.toString().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    };
    private final HttpHandler httpHandler = new HttpHandler() {//the handler is the logic of what to do, in this case, how to handle the requests, and when to throw errors
        @Override
        public void handle(HttpExchange exchange) throws IOException { // the exchange is the connection thats established with the server, it has all the info for the request,
            if(!exchange.getRequestMethod().equalsIgnoreCase("post")){//only accepting post request at the momement
                logger.log(Level.WARNING, "Unsupported HTTP method: " + exchange.getRequestMethod()+" was requested.");
                String response = "Server only accepts POST requests.";
                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                Headers headers = exchange.getResponseHeaders();
                headers.add("Cached-Response", "false");
                exchange.sendResponseHeaders(405, responseBytes.length); //sedning the response coe and the size of the response.
                OutputStream os = exchange.getResponseBody(); //getResponseBody return an output stream
                os.write(responseBytes); //writing the response to the output stream
                os.close();
            }
            else{
                Headers header = exchange.getRequestHeaders(); //gets all the http headers in a Map<String, List<String>> format
                String contentType = header.getFirst("Content-Type");
                if (contentType == null || !contentType.equals("text/x-java-source")){//making sure the header has the value we need
                    logger.log(Level.WARNING, "Unsupported content type: " + contentType+" was requested.");
                    String response = "Content Type "+ contentType +" is not supported.";
                    byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                    Headers headers = exchange.getResponseHeaders();
                    headers.add("Cached-Response", "false");
                    exchange.sendResponseHeaders(400, responseBytes.length);
                    try(OutputStream os = exchange.getResponseBody()){
                        os.write(responseBytes);
                    }
                }
                else{
                    byte[] codeBytes = exchange.getRequestBody().readAllBytes();
                    //byte[] codeBytes = Util.readAllBytesFromNetwork(exchange.getRequestBody());
                    String code = new String(codeBytes, StandardCharsets.UTF_8);
                    logger.info("Gateway read " + codeBytes.length + " bytes");
                    logger.info("Gateway code: [" + code + "]");
                    if(cache.containsKey(code.hashCode())){//this checks if code that client submitted was already submitted before and if it is in the cache
                        String response = cache.get(code.hashCode());
                        if(response.startsWith("SUCCESS:")){
                            String strippedResponse = response.substring(response.indexOf(":") + 1);

                            Headers headers = exchange.getResponseHeaders();
                            headers.add("Cached-Response", "true");

                            byte[] bytes = strippedResponse.getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(200, bytes.length);

                            try (OutputStream os = exchange.getResponseBody()) {
                                os.write(bytes);
                            }
                            return;
                        }
                        else if(response.startsWith("EXCEPTION:")) {
                            String strippedResponse = response.substring(response.indexOf(":") + 1);

                            Headers headers = exchange.getResponseHeaders();
                            headers.add("Cached-Response", "true");

                            byte[] bytes = strippedResponse.getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(400, bytes.length);

                            try (OutputStream os = exchange.getResponseBody()) {
                                os.write(bytes);
                            }
                            return;
                        }
                        else{
                            logger.log(Level.SEVERE,"Something very unusual happened, message didnt have the expected prefix.\nResponse is: " + response);
                            return;
                        }
                    }
                    else{
                        long reqId= requestID.incrementAndGet();
                        Message msg = new Message(Message.MessageType.WORK, code.getBytes(StandardCharsets.UTF_8), gatewayPeerServerImpl.getAddress().getHostName(), gatewayPeerServerImpl.myPort+2, gatewayPeerServerImpl.leaderAddress.getHostName(), gatewayPeerServerImpl.leaderAddress.getPort()+2, reqId);
                        requestIdsToResults.put(reqId, new LinkedBlockingQueue<>());
                        logger.info("Gateway created message with " + msg.getMessageContents().length + " bytes for reqID " + reqId+".\nSending to server "+ gatewayPeerServerImpl.getCurrentLeader().getProposedLeaderID()+".\n");
                        try {
                            gatewayPeerServerImpl.getOutgoingMessagesTCP().put(msg);
                            String result = requestIdsToResults.get(reqId).poll(20, TimeUnit.SECONDS);
                            while(result == null && gatewayPeerServerImpl.leaderAddress!=null) {
                                msg = new Message(Message.MessageType.WORK, code.getBytes(StandardCharsets.UTF_8), gatewayPeerServerImpl.getAddress().getHostName(), gatewayPeerServerImpl.myPort+2, gatewayPeerServerImpl.leaderAddress.getHostName(), gatewayPeerServerImpl.leaderAddress.getPort()+2, reqId);
                                gatewayPeerServerImpl.getOutgoingMessagesTCP().put(msg);
                                result = requestIdsToResults.get(reqId).poll(20, TimeUnit.SECONDS);
                            }
                            requestIdsToResults.remove(reqId);
                            cache.put(code.hashCode(), result);

                            if(result.startsWith("SUCCESS:")){
                                String response = result.substring(result.indexOf(":") + 1);

                                Headers headers = exchange.getResponseHeaders();
                                headers.add("Cached-Response", "false");

                                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                                exchange.sendResponseHeaders(200, bytes.length);

                                try (OutputStream os = exchange.getResponseBody()) {
                                    os.write(bytes);
                                }
                                return;
                            }
                            else if(result.startsWith("EXCEPTION:")) {
                                String strippedResponse = result.substring(result.indexOf(":") + 1);

                                Headers headers = exchange.getResponseHeaders();
                                headers.add("Cached-Response", "false");

                                byte[] bytes = strippedResponse.getBytes(StandardCharsets.UTF_8);
                                exchange.sendResponseHeaders(400, bytes.length);

                                try (OutputStream os = exchange.getResponseBody()) {
                                    os.write(bytes);
                                }
                                return;
                            }
                            else{
                                logger.log(Level.SEVERE,"Something very unusual happened, message didnt have the expected prefix.\nResponse is: " + result);
                                return;
                            }

                        } catch (Exception e) {
                            logger.log(Level.WARNING, "Wasn't able to fullfil client request "+ reqId);
                            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                            try(PrintStream printStream = new PrintStream(byteArrayOutputStream)) { //putting this line of code in () so the stream closes itself right after the block
                                e.printStackTrace(printStream);
                            }
                            String stackTrace = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                            String exceptionMessage = e.getMessage() + "\n" + stackTrace;
                            byte[] exceptionBytes = exceptionMessage.getBytes(StandardCharsets.UTF_8);
                            Headers headers = exchange.getResponseHeaders();
                            headers.add("Cached-Response", "false");
                            exchange.sendResponseHeaders(400, exceptionBytes.length);
                            try(OutputStream os = exchange.getResponseBody()){
                                os.write(exceptionBytes);
                            }
                        }
                    }
                }
            }
        }
    };
    private Thread tcpSender;
    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException{
        try{
            this.logger = initializeLogging(GatewayServer.class.getCanonicalName() + "-on-port-" + peerPort);
            this.logger.setUseParentHandlers(false);
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        this.gatewayPeerServerImpl = new GatewayPeerServerImpl(peerPort,peerEpoch, serverID,peerIDtoAddress, serverID, numberOfObservers);
        this.httpPort = httpPort;
        try {
            this.httpServer= HttpServer.create(new InetSocketAddress(this.httpPort), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.httpServer.createContext("/compileandrun" , httpHandler);
        this.httpServer.createContext("/status", statusHandler);
        // Status endpoint - checks if leader exists and returns node roles
        this.dispatcher = new Thread(()->{
           while(!Thread.currentThread().isInterrupted()){
               try {
                   Message msg = gatewayPeerServerImpl.incomingMessagesTCP.take();
                   InetSocketAddress msgSender = new InetSocketAddress(msg.getSenderHost(),msg.getSenderPort());
                   if(msgSender.getPort()==(this.gatewayPeerServerImpl.leaderAddress.getPort()+2)){//this makes sure that only the currect leader can send results, not potentially dead leader
                       this.requestIdsToResults.get(msg.getRequestID()).put(new String(msg.getMessageContents()));
                   }
               } catch (InterruptedException e) {
                   Thread.currentThread().interrupt();
               }

           }
        });
        dispatcher.setName("Gateway-Dispatcher");
        dispatcher.setDaemon(true);
    }

    public GatewayPeerServerImpl getPeerServer(){
        return gatewayPeerServerImpl;
    }
    @Override
    public void run(){
        this.httpServer.start();
        this.gatewayPeerServerImpl.start();
        this.dispatcher.start();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void shutdown() {
        logger.info("Shutting down GatewayServer");
        if (httpServer != null) {
            httpServer.stop(0);
        }
        if (tcpSender != null && tcpSender.isAlive()) {
            tcpSender.interrupt();
        }
        if (dispatcher != null && dispatcher.isAlive()) {
            dispatcher.interrupt();
        }
        if (gatewayPeerServerImpl != null) {
            gatewayPeerServerImpl.shutdown();
        }
        this.interrupt();
    }

}
