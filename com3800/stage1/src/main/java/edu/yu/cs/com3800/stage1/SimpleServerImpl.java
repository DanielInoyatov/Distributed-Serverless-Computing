package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SimpleServerImpl implements SimpleServer {
    private int port;//the port from which the server will listen on
    HttpHandler httpHandler = new HttpHandler() {//the handler is the logic of what to do, in this case, how to handle the requests, and when to throw errors
        @Override
        public void handle(HttpExchange exchange) throws IOException { // the exchange is the connection thats established with the server, it has all the info for the request,
            if(!exchange.getRequestMethod().equalsIgnoreCase("post")){//only accepting post request at the momement
                String response = "Server only accepts POST requests.";
                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(405, responseBytes.length); //sedning the response coe and the size of the response.
                OutputStream os = exchange.getResponseBody(); //getResponseBody return an output stream
                os.write(responseBytes); //writing the response to the output stream
                os.close();
            }
            else{
                Headers header = exchange.getRequestHeaders(); //gets all the http headers in a Map<String, List<String>> format
                String contentType = header.getFirst("Content-Type");
                if(!contentType.equals("text/x-java-source")){//making sure the header has the value we need
                    String response = "Content Type "+ contentType +" is not supported.";
                    byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(400, responseBytes.length);
                    try(OutputStream os = exchange.getResponseBody()){
                        os.write(responseBytes);
                    }
                }
                else{
                    //use java runner to compile and run code, wrap in a try catch, if exception print
                    Path currentDir = Paths.get(System.getProperty("user.dir"));
                    JavaRunner jr = new JavaRunner(currentDir);
                    try{
                        String outcome = jr.compileAndRun(exchange.getRequestBody());
                        byte[] outcomeBytes = outcome.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, outcomeBytes.length);
                        try(OutputStream os = exchange.getResponseBody()){
                            os.write(outcomeBytes);
                        }
                    }
                    catch (Exception e) {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        try(PrintStream printStream = new PrintStream(byteArrayOutputStream)) { //putting this line of code in () so the stream closes itself right after the block
                            e.printStackTrace(printStream);
                        }
                        String stackTrace = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                        String exceptionMessage = e.getMessage() + "\n" + stackTrace;
                        byte[] exceptionBytes = exceptionMessage.getBytes(StandardCharsets.UTF_8);

                        exchange.sendResponseHeaders(400, exceptionBytes.length);
                        try(OutputStream os = exchange.getResponseBody()){
                            os.write(exceptionBytes);
                        }
                    }
                }
            }
        }
    };
    HttpServer httpServer;
    public SimpleServerImpl(int port) throws IOException {
        this.port = port;
        this.httpServer= HttpServer.create(new InetSocketAddress(this.port), 0);
        this.httpServer.createContext("/compileandrun" , httpHandler); //adding a context to our server
    }
    /**
     * start the server
     */
    @Override
    public void start() {
        this.httpServer.start();
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        this.httpServer.stop(0);
    }

    public static void main(String[] args)
    {
        int port = 9000;
        if(args.length >0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }
}
