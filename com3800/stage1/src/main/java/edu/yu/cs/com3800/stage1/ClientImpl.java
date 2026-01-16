package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ClientImpl implements Client{
    private HttpClient httpClient;
    URI uri;
    HttpResponse<String> response;
    public ClientImpl(String hostName, int hostPort) throws MalformedURLException{
        try{
            this.uri = new URI("http://" + hostName + ":" + hostPort + "/compileandrun");//this identifies the uri of server I am trying to talk to
            httpClient = HttpClient.newHttpClient();
        }
        catch (URISyntaxException e) {
            throw new MalformedURLException(e.getMessage());
        }
    }
    /**
     * @param src
     * @throws IOException
     */
    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(src))//makes the request body the src
                .build();//creates the request
        try{
            this.response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());//sending to the server
        }
        catch(InterruptedException e){
            throw new IOException(e.getMessage());
        }
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public Response getResponse() throws IOException {
        if(this.response==null){
            throw new IOException("response is null");
        }
        return new Response(this.response.statusCode(), this.response.body());
    }
}
