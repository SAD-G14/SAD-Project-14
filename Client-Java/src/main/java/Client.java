import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.lang.reflect.Type;
import java.util.concurrent.Callable;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class Client {

    public static final int TIME_BETWEEN_PULLS = 1;
    private static final String PROTOCOL = "http";
    private static final HashMap REQUEST = new HashMap<String, String>() {{
        put("push", "/queue/push");
        put("pull", "/queue/pull");
        put("ack", "/queue/ack");
        put("health", "/health");
    }};

    private String host = "195.177.255.132";
    private int port = 4000;
    private int sequence_number = 0;
    private long producer_id = System.currentTimeMillis();
    private ArrayList<Thread> threads = new ArrayList<Thread>();

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String push(String key, byte[] value) {
        try {
            String json = "{\"key\":" + key + ",\"value\":" + (new String(value)) + ",\"sequence_number\":" +
                    this.sequence_number + ",\"producer_id\":" + this.producer_id + "}";
            URL obj = new URL(Client.PROTOCOL + "://" + this.host + Client.REQUEST.get("push"));
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(json);
            wr.flush();
            wr.close();

            int responseCode = con.getResponseCode();
            if (responseCode > 299 || responseCode < 200)
                throw new Exception("invalid response" + responseCode);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            return response.toString();
        } catch (Exception e) {
            System.err.println(e);
        }
        return null;
    }

    public Pair<String, byte[]> pull() {
        try {
            String json = "{producer_id:" + this.producer_id + "}";
            URL obj = new URL(Client.PROTOCOL + "://" + this.host + ":" + this.port + Client.REQUEST.get("pull"));
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(json);
            wr.flush();
            wr.close();

            int responseCode = con.getResponseCode();
            if (responseCode > 299 || responseCode < 200)
                throw new Exception("invalid response" + responseCode);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            String response_string = response.toString();
            if (response_string.length() == 0)
                throw new Exception("Empty response");
            Gson gson = new Gson();
            JsonElement jsonElement = JsonParser.parseString(response_string);
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            this.sendAckWithRetry(jsonObject.get("producer_id").getAsInt(),
                    jsonObject.get("sequence_number").getAsInt(), "key");
            Pair<String, byte[]> pair = new Pair<String, byte[]>(jsonObject.get("key").getAsString(),
                    jsonObject.get("value").getAsString().getBytes());
            return pair;
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    private void sendAckWithRetry(int producerId, int sequenceNumber, String key) {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("producer_id", producerId);
        data.put("sequence_number", sequenceNumber);
        data.put("key", key);

        Gson gson = new Gson();
        Type mapType = new TypeToken<Map<String, Object>>(){}.getType();
        String jsonData = gson.toJson(data, mapType);

        int maxAttempts =  5;
        int delay =  2000; // Delay in milliseconds
        int backoff =  2; // Backoff factor

        for (int attempt =  0; attempt < maxAttempts; attempt++) {
            try {
                sendPostRequest(jsonData);
                break; // Break the loop if the request was successful
            } catch (IOException e) {
                if (attempt == maxAttempts -  1) {
                    System.err.println("An error occurred: " + e.getMessage());
                } else {
                    try {
                        Thread.sleep((long) (delay * Math.pow(backoff, attempt)));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        System.err.println("Thread interrupted during sleep: " + ie.getMessage());
                    }
                }
            }
        }
    }

    private void sendPostRequest(String jsonData) throws IOException {
        String urlString = PROTOCOL + "://" + this.host + ":" + this.port + Client.REQUEST.get("ack");
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        try {
            OutputStream outputStream = connection.getOutputStream();
            byte[] input = jsonData.getBytes("utf-8");
            outputStream.write(input,  0, input.length);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        int responseCode = connection.getResponseCode();
        if (responseCode >=  400) {
            throw new IOException("Received error response code: " + responseCode);
        }
    }

    public void subscribe(Callable<Void> func){
        Consumer consumer = new Consumer(this, func);
        Thread thread = new Thread(consumer);
        this.threads.add(thread);
        thread.start();
    }

}