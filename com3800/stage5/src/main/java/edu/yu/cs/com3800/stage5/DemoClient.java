package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Simple client to send compile and run requests to the gateway
 */
public class DemoClient {
    
    private final String gatewayUrl;
    
    public DemoClient(String gatewayHost, int gatewayPort) {
        this.gatewayUrl = "http://" + gatewayHost + ":" + gatewayPort;
    }
    
    /**
     * Send a compile and run request
     */
    public String sendRequest(String javaCode) throws IOException {
        URL url = new URL(gatewayUrl + "/compileandrun");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/x-java-source");
            conn.setDoOutput(true);
            
            // Send the Java code
            try (OutputStream os = conn.getOutputStream()) {
                os.write(javaCode.getBytes());
                os.flush();
            }
            
            // Get response
            int responseCode = conn.getResponseCode();
            StringBuilder response = new StringBuilder();
            
            InputStream inputStream = (responseCode >= 200 && responseCode < 400)
                    ? conn.getInputStream()
                    : conn.getErrorStream();
            
            if (inputStream != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        response.append(line).append("\n");
                    }
                }
            }
            
            return "Response Code: " + responseCode + "\n" + response.toString();
            
        } finally {
            conn.disconnect();
        }
    }
    
    /**
     * Check gateway status
     */
    public String checkStatus() throws IOException {
        URL url = new URL(gatewayUrl + "/status");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        try {
            conn.setRequestMethod("GET");
            
            int responseCode = conn.getResponseCode();
            StringBuilder response = new StringBuilder();
            
            InputStream inputStream = (responseCode >= 200 && responseCode < 400)
                    ? conn.getInputStream()
                    : conn.getErrorStream();
            
            if (inputStream != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        response.append(line).append("\n");
                    }
                }
            }
            
            return response.toString();
            
        } finally {
            conn.disconnect();
        }
    }
    
    /**
     * Main method for command-line usage
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }
        
        try {
            DemoClient client = new DemoClient("localhost", 8080);
            
            String command = args[0];
            
            if ("status".equals(command)) {
                String status = client.checkStatus();
                System.out.println(status);
                
            } else if ("request".equals(command)) {
                if (args.length < 2) {
                    System.err.println("Error: 'request' command requires a number argument");
                    printUsage();
                    System.exit(1);
                }
                int requestNum = Integer.parseInt(args[1]);
                String javaCode = generateTestCode(requestNum);
                
                System.out.println("Sending request #" + requestNum + ":");
                System.out.println(javaCode);
                System.out.println("---");
                
                String response = client.sendRequest(javaCode);
                System.out.println("Response:");
                System.out.println(response);
                
            } else {
                System.err.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
            }
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid number format");
            printUsage();
            System.exit(1);
        }
    }
    
    private static void printUsage() {
        System.err.println("Usage: java DemoClient <command> [arg]");
        System.err.println("Commands:");
        System.err.println("  status              - Check gateway status");
        System.err.println("  request <number>    - Send test request number");
    }
    
    /**
     * Generate sample Java code for testing
     */
    private static String generateTestCode(int num) {
        return "public class TestClass" + num + " {\n" +
               "    public String run() {\n" +
               "        return \"Hello from request " + num + "!\";\n" +
               "    }\n" +
               "}";
    }
}