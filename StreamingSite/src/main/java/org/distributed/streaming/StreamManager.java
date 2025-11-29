package org.distributed.streaming;

import org.red5.server.adapter.MultiThreadedApplicationAdapter;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class StreamManager extends MultiThreadedApplicationAdapter{

    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);
    
    static {
        System.out.println("========================================");
        System.out.println("StreamManager class is being loaded!");
        System.out.println("========================================");
    }
    
    public StreamManager() {
        System.out.println("========================================");
        System.out.println("StreamManager constructor called!");
        System.out.println("========================================");
        logger.info("StreamManager instance created");
    }

    @Override
    public boolean appStart(IScope app){
        System.out.println("live appStart");
        logger.info("StreamManager application started for scope: {}", app.getName());
        return super.appStart(app);
    }

    @Override
    public void streamPublishStart(IBroadcastStream stream){
        logger.info("Stream published: {}", stream.getPublishedName());
        stream.addStreamListener(new KafkaPacketListener());
        super.streamPublishStart(stream);
    }

    @Override
    public void appStop(IScope app){
        logger.info("StreamManager application stopped.");
        super.appStop(app);
    }

    private static class KafkaPacketListener implements IStreamListener{
        private long packetCount = 0;

        @Override
        public void packetReceived(IBroadcastStream stream, IStreamPacket packet) {
            packetCount++;

            if (packetCount % 1000 == 0) {
                System.out.println("Packet received! Type: " + packet.getDataType() + "Size: " + packet.getData().limit());
            }

            //TODO: Send packets to Kafka 
        }
    }
    
}
