package org.distributed.streaming;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.red5.server.adapter.MultiThreadedApplicationAdapter;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class StreamManager extends MultiThreadedApplicationAdapter {

    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);
    private KafkaProducer<String, byte[]> producer;
    private final String TOPIC_NAME = "live-stream";
    private final String VALIDATION_API_URL = "http://localhost:8080/api/stream/check-key/";

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final OkHttpClient httpClient = new OkHttpClient();
    private final Gson gson = new Gson();

    @Override
    public boolean appStart(IScope app) {
        executor.submit(()->{
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

            try {
                producer = new KafkaProducer<>(props);
                logger.info("Kafka Producer initialized.");
                System.out.println("Kafka Producer initialized.");
            } catch (Exception e) {
                logger.error("Failed to initialize Kafka Producer.", e);
                System.out.println("Failed to initialize Kafka Producer: " + e.getMessage());
            }
        });
        logger.info("StreamManager application started.");
        return super.appStart(app);
    }

    /**
     * Validate stream key by calling the Spring Boot API
     */
    private StreamKeyValidationResult validateStreamKey(String streamKey) {
        System.out.println("=== VALIDATING STREAM KEY ===");
        System.out.println("Stream key: " + streamKey);
        System.out.println("API URL: " + VALIDATION_API_URL + streamKey);
        
        try {
            Request request = new Request.Builder()
                .url(VALIDATION_API_URL + streamKey)
                .get()
                .build();

            try (Response response = httpClient.newCall(request).execute()) {
                System.out.println("API Response code: " + response.code());
                
                if (response.isSuccessful() && response.body() != null) {
                    String responseBody = response.body().string();
                    System.out.println("API Response body: " + responseBody);
                    
                    JsonObject json = gson.fromJson(responseBody, JsonObject.class);
                    
                    boolean valid = json.get("valid").getAsBoolean();
                    if (valid) {
                        Long userId = json.get("userId").getAsLong();
                        String username = json.get("username").getAsString();
                        logger.info("Stream key validated successfully for user: {} (ID: {})", username, userId);
                        System.out.println("✓ Stream key validated for user: " + username + " (ID: " + userId + ")");
                        return new StreamKeyValidationResult(true, userId, username);
                    } else {
                        logger.warn("Invalid stream key: {}", streamKey);
                        System.out.println("✗ Invalid stream key: " + streamKey);
                        return new StreamKeyValidationResult(false, null, null);
                    }
                } else {
                    logger.error("Failed to validate stream key. HTTP status: {}", response.code());
                    System.out.println("✗ Failed to validate stream key. HTTP status: " + response.code());
                    if (response.body() != null) {
                        System.out.println("Response body: " + response.body().string());
                    }
                    return new StreamKeyValidationResult(false, null, null);
                }
            }
        } catch (Exception e) {
            logger.error("Error validating stream key", e);
            System.out.println("✗ Error validating stream key: " + e.getMessage());
            e.printStackTrace();
            return new StreamKeyValidationResult(false, null, null);
        }
    }

    @Override
    public void streamPublishStart(IBroadcastStream stream) {
        String streamName = stream.getPublishedName();
        logger.info("Stream publish start request: {}", streamName);
        System.out.println("Stream publish start request: " + streamName);

        // Validate the stream key
        StreamKeyValidationResult validationResult = validateStreamKey(streamName);
        
        if (!validationResult.isValid()) {
            logger.warn("Rejecting stream - invalid stream key: {}", streamName);
            System.out.println("Rejecting stream - invalid stream key: " + streamName);
            // Close the stream
            stream.close();
            return;
        }

        logger.info("Stream start authorized for user: {} ({})", validationResult.getUsername(), streamName);
        System.out.println("Stream start authorized for user: " + validationResult.getUsername() + " (" + streamName + ")");

        executor.submit(()->{
            if(producer != null){
                try{
                    // Send START message with user info
                    String message = String.format("START|%d|%s", 
                        validationResult.getUserId(), 
                        validationResult.getUsername());
                    
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        TOPIC_NAME, 
                        streamName, 
                        message.getBytes()
                    );
                    producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            logger.error("Error sending message to Kafka for stream: " + streamName, exception);
                            System.out.println("Error sending message to Kafka for stream: " + streamName + " Exception: " + exception.getMessage());
                        } else {
                            logger.info("Message sent to Kafka for stream: " + streamName + 
                                " User: " + validationResult.getUsername() +
                                " Topic: " + metadata.topic() + 
                                " Partition: " + metadata.partition() + 
                                " Offset: " + metadata.offset());
                            System.out.println("Message sent to Kafka for stream: " + streamName + 
                                " User: " + validationResult.getUsername() +
                                " Topic: " + metadata.topic() + 
                                " Partition: " + metadata.partition() + 
                                " Offset: " + metadata.offset());
                        }
                    });
                } catch (Exception e) {
                    logger.error("Exception while sending message to Kafka for stream: " + streamName, e);
                    System.out.println("Exception while sending message to Kafka for stream: " + streamName + " Exception: " + e.getMessage());
                }
            }
        });
        
        super.streamPublishStart(stream);
    }

    @Override
    public void streamBroadcastClose(IBroadcastStream stream) {
        String streamName = stream.getPublishedName();
        logger.info("Stream stopped: {}", streamName);
        System.out.println("Stream stopped: " + streamName);
        
        executor.submit(() -> {
            if (producer != null) {
                try {
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        TOPIC_NAME, 
                        streamName, 
                        "STOP".getBytes()
                    );
                    producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            logger.error("Error sending STOP message to Kafka for stream: " + streamName, exception);
                            System.out.println("Error sending STOP message to Kafka for stream: " + streamName);
                        } else {
                            logger.info("STOP message sent to Kafka for stream: " + streamName);
                            System.out.println("STOP message sent to Kafka for stream: " + streamName);
                        }
                    });
                } catch (Exception e) {
                    logger.error("Exception while sending STOP message to Kafka for stream: " + streamName, e);
                    System.out.println("Exception while sending STOP message to Kafka: " + e.getMessage());
                }
            }
        });
        
        super.streamBroadcastClose(stream);
    }

    @Override
    public void appStop(IScope app) {
        if (producer != null) {
            producer.close();
            logger.info("Kafka Producer closed.");
            System.out.println("Kafka Producer closed.");
        }
        executor.shutdown();
        logger.info("StreamManager application stopped.");
        System.out.println("StreamManager application stopped.");
        super.appStop(app);
    }

    /**
     * Inner class to hold validation results
     */
    private static class StreamKeyValidationResult {
        private final boolean valid;
        private final Long userId;
        private final String username;

        public StreamKeyValidationResult(boolean valid, Long userId, String username) {
            this.valid = valid;
            this.userId = userId;
            this.username = username;
        }

        public boolean isValid() {
            return valid;
        }

        public Long getUserId() {
            return userId;
        }

        public String getUsername() {
            return username;
        }

        public String toString() {
            return "StreamKeyValidationResult{valid=" + valid + ", userId=" + userId + ", username='" + username + "'}";
        }

    }
}
