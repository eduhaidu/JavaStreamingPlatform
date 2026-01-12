/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package org.distributed.transcoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.messages.Item;

/**
 *
 * @author eduhaidu
 */
public class TranscodingWorker {
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final Set<String> uploadedFiles = ConcurrentHashMap.newKeySet();
    private static final ConcurrentHashMap<String, Process> activeProcesses = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Thread> monitorThreads = new ConcurrentHashMap<>();
    private static final Set<String> activeStreams = ConcurrentHashMap.newKeySet();
    private static volatile boolean running = true;
    private static KafkaProducer<String, String> producer;
    private static final String KAFKA_TOPIC = "live-stream";

    private static final String MINIO_URL = "http://localhost:9000";
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin123";
    private static final String MINIO_BUCKET = "video-storage";

    public static void main(String[] args) {
        // Add shutdown hook to cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down TranscodingWorker...");
            running = false;
            stopAllProcesses();
            if (producer != null) {
                producer.close();
            }
            executor.shutdown();
        }));

        // Initialize Kafka producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "transcoding-worker-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(java.util.Collections.singletonList("live-stream"));

        System.out.println("Transcoding Worker started and listening to 'live-stream' topic...");

        try {
            while (running) { 
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records){
                    String streamName = record.key();
                    String action = record.value();

                    System.out.println("Received message for stream: " + streamName + " with action: " + action);

                    // Handle new message format: "START|userId|username" or old format "START"
                    if(action.startsWith("START")){
                        executor.submit(() -> startStreaming(streamName));
                    } else if("STOP".equals(action)) {
                        stopStreaming(streamName);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void stopStreaming(String streamName) {
        System.out.println("Stopping stream: " + streamName);
        
        // Mark stream as inactive to stop monitoring
        activeStreams.remove(streamName);
        
        // Stop FFmpeg process
        Process process = activeProcesses.remove(streamName);
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
                System.out.println("FFmpeg process stopped for stream: " + streamName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Stop monitor thread
        Thread monitorThread = monitorThreads.remove(streamName);
        if (monitorThread != null && monitorThread.isAlive()) {
            monitorThread.interrupt();
            System.out.println("Monitor thread stopped for stream: " + streamName);
        }
        
        // Archive stream for VOD
        executor.submit(() -> archiveStream(streamName));
    }
    
    private static void archiveStream(String streamName) {
        try {
            System.out.println("Archiving stream: " + streamName);
            Thread.sleep(2000); // Wait for final segments to be written
            
            String outputDir = "hls";
            File dir = new File(outputDir);
            File[] files = dir.listFiles((d, name) -> name.startsWith(streamName));
            
            if (files == null || files.length == 0) {
                System.out.println("No files found to archive for: " + streamName);
                return;
            }
            
            MinioClient minioClient = MinioClient.builder()
                .endpoint(MINIO_URL)
                .credentials(MINIO_USER, MINIO_PASSWORD)
                .build();
            
            String vodPrefix = "vod/" + streamName + "_" + System.currentTimeMillis() + "/";
            long totalSize = 0;
            
            // Upload all segments and playlist with VOD prefix
            for (File file : files) {
                String vodObjectName = vodPrefix + file.getName();
                String contentType = file.getName().endsWith(".m3u8") ? 
                    "application/x-mpegURL" : "video/MP2T";
                
                minioClient.uploadObject(
                    UploadObjectArgs.builder()
                        .bucket(MINIO_BUCKET)
                        .object(vodObjectName)
                        .filename(file.getAbsolutePath())
                        .contentType(contentType)
                        .build()
                );
                totalSize += file.length();
                System.out.println("Archived: " + vodObjectName);
            }
            
            System.out.println("✓ Stream archived successfully: " + vodPrefix);
            
            // Delete live stream chunks from MinIO to prevent old footage from appearing
            System.out.println("Cleaning up live stream chunks from MinIO...");
            try {
                Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                        .bucket(MINIO_BUCKET)
                        .prefix(streamName)
                        .build()
                );
                
                int deletedCount = 0;
                for (Result<Item> result : results) {
                    Item item = result.get();
                    String objectName = item.objectName();
                    
                    // Only delete objects that don't have the vod/ prefix (live stream chunks)
                    if (!objectName.startsWith("vod/")) {
                        minioClient.removeObject(
                            RemoveObjectArgs.builder()
                                .bucket(MINIO_BUCKET)
                                .object(objectName)
                                .build()
                        );
                        deletedCount++;
                        System.out.println("Deleted live chunk: " + objectName);
                    }
                }
                System.out.println("✓ Cleaned up " + deletedCount + " live stream chunks from MinIO");
            } catch (Exception cleanupEx) {
                System.err.println("Warning: Failed to cleanup live stream chunks from MinIO");
                cleanupEx.printStackTrace();
            }
            
            // Send Kafka message with VOD details
            String vodMessage = "ARCHIVE|" + streamName + "|" + vodPrefix + "|" + totalSize;
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, vodMessage), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("VOD archive message sent to Kafka: " + vodMessage);
                } else {
                    System.err.println("Failed to send VOD archive message");
                    exception.printStackTrace();
                }
            });
            
            // Cleanup local files after archiving
            for (File file : files) {
                file.delete();
            }
            
        } catch (Exception e) {
            System.err.println("Failed to archive stream: " + streamName);
            e.printStackTrace();
        }
    }

    private static void stopAllProcesses() {
        System.out.println("Stopping all active processes...");
        for (String streamName : activeProcesses.keySet()) {
            stopStreaming(streamName);
        }
    }

    private static void startStreaming(String streamName){
        // Mark stream as active
        activeStreams.add(streamName);
        
        String rtmpUrl = "rtmp://localhost/live/" + streamName;
        String outputFilename = streamName + ".m3u8";
        String outputDir = "hls";
        Path localPath = Paths.get(outputDir, outputFilename);

        File dir = new File(outputDir);
        if (!dir.exists()){
            dir.mkdirs();
        }

        MinioClient minioClient = MinioClient.builder()
            .endpoint(MINIO_URL)
            .credentials(MINIO_USER, MINIO_PASSWORD)
            .build();

        Thread monitorThread = new Thread(() -> monitorAndUpload(minioClient, outputDir, streamName));
        monitorThread.setDaemon(false);
        monitorThread.start();
        monitorThreads.put(streamName, monitorThread);

        System.out.println("Starting FFmpeg for stream: " + streamName);
        ProcessBuilder pb = new ProcessBuilder(
            "ffmpeg",
            "-i", rtmpUrl,
            "-c:v", "libx264", "-preset", "veryfast", "-b:v", "3000k",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", "4",
            "-hls_list_size", "5",
            "-hls_playlist_type", "event",  // Creates VOD-compatible playlist
            localPath.toString()
        );

        try {
            Process process = pb.start();
            activeProcesses.put(streamName, process);

            new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))){
                    while(reader.readLine() != null){
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

            int exitCode = process.waitFor();
            System.out.println("Transcoding finished with code: " + exitCode);
            
            // Cleanup
            activeStreams.remove(streamName);
            activeProcesses.remove(streamName);
            Thread monitor = monitorThreads.remove(streamName);
            if (monitor != null) {
                monitor.interrupt();
            }
        } catch (Exception e) {
            e.printStackTrace();
            activeProcesses.remove(streamName);
        }
    }

    private static void monitorAndUpload(MinioClient client, String directory, String streamName){
        File dir = new File(directory);
        System.out.println("Upload monitor started for directory: " + directory);

        while (running && !Thread.currentThread().isInterrupted() && activeStreams.contains(streamName)) { 
            File[] files = dir.listFiles();
            if (files!=null){
                for (File file : files){
                    if (!file.getName().startsWith(streamName)) continue;

                    try {
                        // Wait for file to be fully written (check size stability)
                        long size1 = file.length();
                        Thread.sleep(100);
                        long size2 = file.length();
                        
                        if (size1 != size2) {
                            continue; // File still being written
                        }

                        if(file.getName().endsWith(".m3u8")){
                            uploadFile(client, file, "application/x-mpegURL");
                        }
                        else if (file.getName().endsWith(".ts")){
                            String fileKey = streamName + ":" + file.getName();
                            if(!uploadedFiles.contains(fileKey)){
                                uploadFile(client, file, "video/MP2T");
                                uploadedFiles.add(fileKey);
                                System.out.println("Uploaded new segment: " + file.getName());
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.err.println("Failed to upload file: " + file.getName() + " Error: " + e.getMessage());
                    }
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        System.out.println("Upload monitor stopped for stream: " + streamName);
    }

    private static void uploadFile(MinioClient client, File file, String contentType) throws Exception {
        client.uploadObject(
            UploadObjectArgs.builder()
                .bucket(MINIO_BUCKET)
                .object(file.getName())
                .filename(file.getAbsolutePath())
                .contentType(contentType)
                .build()
        );
        System.out.println("Uploaded file to MinIO: " + file.getName());
    }
}
