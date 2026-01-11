package com.distributed.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.distributed.streaming.entity.User;
import com.distributed.streaming.repository.UserRepository;

@Service
public class StreamEventListener {
    private final StreamRepository streamRepository;
    private final UserRepository userRepository;

    public StreamEventListener(StreamRepository streamRepository, UserRepository userRepository) {
        this.streamRepository = streamRepository;
        this.userRepository = userRepository;
        System.out.println("StreamEventListener initialized - ready to consume Kafka messages");
    }

    @KafkaListener(topics = "live-stream", groupId = "streaming-group")
    public void listen(ConsumerRecord<String, String> record) {
        String streamName = record.key();
        String message = record.value();
        
        System.out.println("=== KAFKA MESSAGE RECEIVED ===");
        System.out.println("Stream Name: " + streamName);
        System.out.println("Message: " + message);

        // Parse message format: "START|userId|username" or "STOP"
        if(message.startsWith("START")){
            System.out.println("Stream started: " + streamName);
            
            // Parse user info from message
            String[] parts = message.split("\\|");
            if (parts.length >= 3) {
                try {
                    Long userId = Long.parseLong(parts[1]);
                    String username = parts[2];
                    
                    // Find user by ID
                    User user = userRepository.findById(userId).orElse(null);
                    
                    if (user != null) {
                        // Create StreamMetadata with user association
                        StreamMetadata streamMetadata = new StreamMetadata();
                        streamMetadata.setStreamName(streamName);
                        streamMetadata.setLive(true);
                        streamMetadata.setStartTime(java.time.LocalDateTime.now());
                        streamMetadata.setUser(user);
                        streamMetadata.setStreamTitle(username + "'s stream"); // Default title
                        streamMetadata.setViewerCount(0);
                        
                        streamRepository.save(streamMetadata);
                        System.out.println("Stream associated with user: " + username + " (ID: " + userId + ")");
                    } else {
                        System.out.println("Warning: User not found with ID: " + userId);
                        // Still create stream metadata without user
                        StreamMetadata streamMetadata = new StreamMetadata(streamName, true, java.time.LocalDateTime.now());
                        streamRepository.save(streamMetadata);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Error parsing user ID from message: " + message);
                    // Fallback: create stream without user association
                    StreamMetadata streamMetadata = new StreamMetadata(streamName, true, java.time.LocalDateTime.now());
                    streamRepository.save(streamMetadata);
                }
            } else {
                // Old message format without user info
                System.out.println("Message without user info, creating stream without user association");
                StreamMetadata streamMetadata = new StreamMetadata(streamName, true, java.time.LocalDateTime.now());
                streamRepository.save(streamMetadata);
            }
        } else if ("STOP".equals(message)) {
            System.out.println("Stream stopped: " + streamName);
            
            // Find the stream and mark it as no longer live
            StreamMetadata stream = streamRepository.findByStreamName(streamName);
            if (stream != null) {
                stream.setLive(false);
                stream.setEndTime(java.time.LocalDateTime.now());
                streamRepository.save(stream);
                System.out.println("Stream marked as ended in database: " + streamName);
            } else {
                System.out.println("Warning: Stream not found in database: " + streamName);
            }
        }
    }
}
