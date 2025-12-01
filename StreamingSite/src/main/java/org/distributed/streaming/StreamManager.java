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

public class StreamManager extends MultiThreadedApplicationAdapter {

    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);
    private KafkaProducer<String, byte[]> producer;
    private final String TOPIC_NAME = "live-stream";

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

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

    // @Override
    // public boolean appConnect(IConnection conn, Object[] params) {
    //     logger.info("Client connecting - ID: {}, Remote: {}", 
    //         conn.getClient().getId(), 
    //         conn.getRemoteAddress());
    //     System.out.println("Client connecting - ID: " + conn.getClient().getId() + 
    //         " Remote: " + conn.getRemoteAddress());
    //     return super.appConnect(conn, params);
    // }

    @Override
    public void streamPublishStart(IBroadcastStream stream) {
        logger.info("Stream start detected: " + stream.getPublishedName() );
        System.out.println("Stream start detected: " + stream.getPublishedName() );

        executor.submit(()->{
            if(producer != null){
                try{
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        TOPIC_NAME, 
                        stream.getPublishedName(), 
                        "START".getBytes()
                    );
                    producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            logger.error("Error sending message to Kafka for stream: " + stream.getPublishedName(), exception);
                            System.out.println("Error sending message to Kafka for stream: " + stream.getPublishedName() + " Exception: " + exception.getMessage());
                        } else {
                            logger.info("Message sent to Kafka for stream: " + stream.getPublishedName() + 
                                " Topic: " + metadata.topic() + 
                                " Partition: " + metadata.partition() + 
                                " Offset: " + metadata.offset());
                            System.out.println("Message sent to Kafka for stream: " + stream.getPublishedName() + 
                                " Topic: " + metadata.topic() + 
                                " Partition: " + metadata.partition() + 
                                " Offset: " + metadata.offset());
                        }
                    });
                } catch (Exception e) {
                    logger.error("Exception while sending message to Kafka for stream: " + stream.getPublishedName(), e);
                    System.out.println("Exception while sending message to Kafka for stream: " + stream.getPublishedName() + " Exception: " + e.getMessage());
                }
            }
        });
        
        super.streamPublishStart(stream);
    }

    // @Override
    // public void streamBroadcastClose(IBroadcastStream stream) {
    //     String streamName = stream.getPublishedName();
    //     logger.info("Stream stopped: {}", streamName);
    //     System.out.println("Stream stopped: " + streamName);
        
    //     if (producer != null) {
    //         ProducerRecord<String, byte[]> record = new ProducerRecord<>(
    //             TOPIC_NAME, 
    //             streamName, 
    //             "STOP".getBytes()
    //         );
    //         producer.send(record);
    //     }
        
    //     super.streamBroadcastClose(stream);
    // }

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

    // @Override
    // public boolean isPublishAllowed(IScope scope, String name, String mode) {
    //     // Allow all publish requests
    //     return true;
    // }
}
