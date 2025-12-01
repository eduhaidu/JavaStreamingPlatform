package org.distributed.streaming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.server.adapter.MultiThreadedApplicationAdapter;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class StreamManager extends MultiThreadedApplicationAdapter{

    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);
    private KafkaProducer<String, byte[]> producer;
    private final String TOPIC_NAME = "live-stream";

    @Override
    public boolean appStart(IScope app){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try {
            this.producer = new KafkaProducer<>(props);
            System.out.println("Kafka Producer initialized.");
        } catch (Exception e) {
            logger.error("Failed to initialize Kafka Producer", e);
            System.out.println("Failed to initialize Kafka Producer: " + e.getMessage());
            return false;
        }
        System.out.println("live appStart");
        logger.info("StreamManager application started for scope: {}", app.getName());
        return super.appStart(app);
    }

    @Override
    public void streamPublishStart(IBroadcastStream stream){
        logger.info("Stream published: {}", stream.getPublishedName());
        System.out.println("Stream published: " + stream.getPublishedName());
        stream.addStreamListener(new KafkaPacketListener(producer, stream.getPublishedName(), TOPIC_NAME));
        super.streamPublishStart(stream);
    }

    @Override
    public void appStop(IScope app){
        if (producer != null) {
            producer.close();
            System.out.println("Kafka Producer closed.");
        }
        logger.info("StreamManager application stopped.");
        super.appStop(app);
    }

    private static class KafkaPacketListener implements IStreamListener{
        private final KafkaProducer<String, byte[]> producer;
        private final String streamName;
        private final String topic;
        private long packetCount = 0;

        public KafkaPacketListener(KafkaProducer<String, byte[]> producer, String streamName, String topic) {
            this.producer = producer;
            this.streamName = streamName;
            this.topic = topic;
        }

        @Override
        public void packetReceived(IBroadcastStream stream, IStreamPacket packet) {
            if(producer == null) {
                System.out.println("Kafka Producer is not initialized.");
                return;
            }
            try {
                IoBuffer buffer = packet.getData();
                byte[] data = new byte[buffer.limit()];
                buffer.get(data);
                buffer.rewind();
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, streamName, data);
                producer.send(record);

                packetCount++;
                if (packetCount % 100 == 0) {
                    System.out.println("Sent " + packetCount + " packets for stream: " + streamName);
                }
            } catch (Exception e) {
                LoggerFactory.getLogger(KafkaPacketListener.class).error("Failed to send packet to Kafka for stream: " + streamName, e);
                System.out.println("Failed to send packet to Kafka for stream: " + streamName + " Error: " + e.getMessage());
            }
        }
    }
    
}
