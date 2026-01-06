package com.distributed.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class StreamEventListener {
    private final StreamRepository streamRepository;

    public StreamEventListener(StreamRepository streamRepository) {
        this.streamRepository = streamRepository;
    }

    @KafkaListener(topics = "live-stream", groupId = "streaming-group")
    public void listen(ConsumerRecord<String, String> record) {
        String streamName = record.key();
        String action = record.value();

        if("START".equals(action)){
            System.out.println("Stream started: " + streamName);
            streamRepository.save(new StreamMetadata(streamName, true, java.time.LocalDateTime.now()));
        }
    }
}
