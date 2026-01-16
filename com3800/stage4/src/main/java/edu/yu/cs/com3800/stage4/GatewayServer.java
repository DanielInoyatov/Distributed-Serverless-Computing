package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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
    private final HttpHandler httpHandler = new HttpHandler() {//the handler is the logic of what to do, in this case, how to handle the requests, and when to throw errors
        @Override
        public void handle(HttpExchange exchange) throws IOException { // the exchange is the connection thats established with the server, it has all the info for the request,
            if(!exchange.getRequestMethod().equalsIgnoreCase("post")){//only accepting post request at the momement
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
                    if(cache.containsKey(code.hashCode())){
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
                        requestIdsToResults.put(reqId, new LinkedBlockingQueue<>());
                        Message msg = new Message(Message.MessageType.WORK, code.getBytes(StandardCharsets.UTF_8), gatewayPeerServerImpl.getAddress().getHostName(), gatewayPeerServerImpl.myPort+2, gatewayPeerServerImpl.leaderAddress.getHostName(), gatewayPeerServerImpl.leaderAddress.getPort()+2, reqId);
                        logger.info("Gateway created message with " + msg.getMessageContents().length + " bytes for reqID " + reqId);
                        try {
                            gatewayPeerServerImpl.getOutgoingMessagesTCP().put(msg);
                            String result = requestIdsToResults.get(reqId).take();
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
        this.dispatcher = new Thread(()->{
           while(!Thread.currentThread().isInterrupted()){
               try {
                   Message msg = gatewayPeerServerImpl.incomingMessagesTCP.take();
                   this.requestIdsToResults.get(msg.getRequestID()).put(new String(msg.getMessageContents()));
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
        Thread.currentThread().interrupt();
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

    }

}
