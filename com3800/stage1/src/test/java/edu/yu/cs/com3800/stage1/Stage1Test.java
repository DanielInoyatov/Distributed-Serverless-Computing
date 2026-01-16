package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.HttpContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class Stage1Test {
    SimpleServerImpl server;
    ClientImpl client;
    URI uri;

    Stage1Test() throws IOException, URISyntaxException {
        server= new SimpleServerImpl(9000);
        client = new ClientImpl("localhost", 9000);
        this.uri = new URI("http://" + "localhost" + ":" + 9000+"/compileandrun");
    }
    @BeforeEach
    void setUp() throws IOException {
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }

    @Test
    void errorHandlingTest() throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(uri)
                .header("Content-Type", "text/x-java-source")
                .GET()
                .build();
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> response = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        System.out.println("==================================================================");
        System.out.println("Comparing expected response body if a GET is sent instead of a post\n");
        System.out.println("Expected response:\n[Server only accepts POST requests.]\nActual response:\n[" + response.body()+"]");
        assertEquals("Server only accepts POST requests.", response.body());
        System.out.println("==================================================================");
        System.out.println("\n");
        System.out.println("==================================================================");
        System.out.println("Comparing expected response code if a GET is sent instead of a post\n");
        System.out.println("Expected response:\n[405]\nActual response:\n[" + response.statusCode()+"]");
        assertEquals(405, response.statusCode());
        System.out.println("==================================================================");

        System.out.println("\n");

        //asserting wrong content types are giving proper responses
        req = HttpRequest.newBuilder(uri)
                .header("Content-Type", "text/x-wrong-source")
                .POST(HttpRequest.BodyPublishers.ofString("won't work"))
                .build();
        response = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        System.out.println("==================================================================");
        System.out.println("Comparing expected response body if wrong content type is sent.\n");
        System.out.println("Expected response:\n[Content Type text/x-wrong-source is not supported.]\nActual response:\n[" + response.body()+"]");
        assertEquals("Content Type text/x-wrong-source is not supported.", response.body());
        System.out.println("==================================================================");

        System.out.println("\n");

        System.out.println("==================================================================");
        System.out.println("Comparing expected response code if the content type is not text/x-java-source\n");
        System.out.println("Expected response:\n[400]\nActual response:\n[" + response.statusCode()+"]");
        assertEquals(400, response.statusCode());
        System.out.println("==================================================================");
        System.out.println("\n");
    }

    @Test
    void clientSendingToServer() throws IOException {
        Path filePath = Path.of("src/test/java/edu/yu/cs/com3800/stage1/SimpleClass.java");
        String fileContent = Files.readString(filePath);
        client.sendCompileAndRunRequest(fileContent);
        Client.Response response = client.getResponse();

        System.out.println("==================================================================");
        System.out.println("Comparing expected response code if the client sent a retirieved succesfully\n");
        System.out.println("Expected response:\n[200]\nActual response:\n[" + response.getCode()+"]");
        assertEquals(200, response.getCode());
        System.out.println("==================================================================");

        System.out.println("\n");

        System.out.println("==================================================================");
        System.out.println("Comparing expected response if the client sent a retirieved succesfully\n");
        System.out.println("Expected response:\n[Daniel]\nActual response:\n[" + response.getBody()+"]");
        assertEquals("Daniel", response.getBody());
        System.out.println("==================================================================");

        filePath = Path.of("src/test/java/edu/yu/cs/com3800/stage1/AnotherClass.java");
        fileContent = Files.readString(filePath);
        client.sendCompileAndRunRequest(fileContent);
        response = client.getResponse();

        System.out.println("==================================================================");
        System.out.println("Comparing expected response code if the client sent another request\n");
        System.out.println("Expected response:\n[200]\nActual response:\n[" + response.getCode()+"]");
        assertEquals(200, response.getCode());
        System.out.println("==================================================================");

        System.out.println("\n");

        System.out.println("==================================================================");
        System.out.println("Comparing expected response if the client sent another request\n");
        System.out.println("Expected response:\n[Inoyatov]\nActual response:\n[" + response.getBody()+"]");
        assertEquals("Inoyatov", response.getBody());
        System.out.println("==================================================================");

        System.out.println("\n");

        filePath = Path.of("src/test/java/edu/yu/cs/com3800/stage1/ProblemClass.java");
        fileContent = Files.readString(filePath);
        client.sendCompileAndRunRequest(fileContent);
        response = client.getResponse();

        System.out.println("==================================================================");
        System.out.println("Comparing expected code if the client sent a bad request\n");
        System.out.println("Expected response:\n[400]\nActual response:\n[" + response.getCode()+"]");
        assertEquals(400, response.getCode());
        System.out.println("==================================================================");
        System.out.println("\n");
        System.out.println("==================================================================");
        System.out.println("Comparing expected body if the client sent a bad request\n");
        assertTrue(response.getBody().contains("ReflectiveOperationException"));
        System.out.println("==================================================================");

    }
}
/*
        Path filePath = Path.of("src/test/java/edu/yu/cs/com3800/stage1/ProblemClass.java");
        String fileContent = Files.readString(filePath);
        client.sendCompileAndRunRequest(fileContent);
 */