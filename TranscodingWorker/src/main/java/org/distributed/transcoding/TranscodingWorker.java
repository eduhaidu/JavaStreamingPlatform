/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package org.distributed.transcoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 *
 * @author eduhaidu
 */
public class TranscodingWorker {
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "transcoding-worker-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(java.util.Collections.singletonList("live-stream"));

        System.out.println("Transcoding Worker started and listening to 'live-stream' topic...");

        try {
            while (true) { 
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records){
                    String streamName = record.key();
                    String action = record.value();

                    System.out.println("Received message for stream: " + streamName + " with action: " + action);

                    if("START".equals(action)){
                        executor.submit(() -> startFFmpeg(streamName));
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void startFFmpeg(String streamName){
        String rtmpUrl = "rtmp://localhost/live/" + streamName;
        String outputPath = "hls/" + streamName + ".m3u8";

        new File("hls").mkdirs();

        System.out.println("Starting FFmpeg for stream: " + streamName);

        ProcessBuilder pb = new ProcessBuilder(
            "ffmpeg",
            "-i", rtmpUrl,
            "-c:v", "libx264", "-preset", "veryfast", "-b:v", "3000k",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", "4",
            "-hls_list_size", "5",
            "-hls_flags", "delete_segments",
            outputPath
        );

        try {
            Process process = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = process.waitFor();
            System.out.println("FFmpeg process for stream " + streamName + " exited with code: " + exitCode);
        } catch (IOException | InterruptedException e) {
        }
    }
}
