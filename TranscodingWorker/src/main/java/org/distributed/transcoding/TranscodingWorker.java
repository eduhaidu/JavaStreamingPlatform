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
import org.apache.kafka.common.serialization.StringDeserializer;

import io.minio.MinioClient;
import io.minio.UploadObjectArgs;

/**
 *
 * @author eduhaidu
 */
public class TranscodingWorker {
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final Set<String> uploadedFiles = ConcurrentHashMap.newKeySet();

    private static final String MINIO_URL = "http://localhost:9000";
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin123";
    private static final String MINIO_BUCKET = "video-storage";

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
                        executor.submit(() -> startStreaming(record.key()));
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void startStreaming(String streamName){
        String rtmpUrl = "rtmp://localhost/live/" + streamName;
        String outputDir = "hls";
        String fileName = streamName + ".m3u8";
        Path localPath = Paths.get(outputDir, fileName);

        new File(outputDir).mkdirs();

        MinioClient minioClient = MinioClient.builder()
            .endpoint(MINIO_URL)
            .credentials(MINIO_USER, MINIO_PASSWORD)
            .build();

        executor.submit(() -> monitorAndUpload(minioClient, outputDir, streamName));

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
            localPath.toString()
        );

        try {
            Process process = pb.start();
            new Thread(()->{
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))){
                    while(reader.readLine() != null){
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }}).start();

            int exitCode = process.waitFor();
            System.out.println("Transcoding finished with code: " + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void monitorAndUpload(MinioClient client, String directory, String streamName){
        File dir = new File(directory);
        System.out.println("Upload monitor started for directory: " + directory);

        while (true) { 
            File[] files = dir.listFiles();
            if (files!=null){
                for (File file : files){
                    if (!file.getName().startsWith(streamName)) continue;

                    try {
                        if(file.getName().endsWith(".m3u8")){
                            uploadFile(client, file, "application/x-mpegURL");
                        }
                        else if (file.getName().endsWith(".ts")){
                            if(!uploadedFiles.contains(file.getName())){
                                uploadFile(client, file, "video/MP2T");
                                uploadedFiles.add(file.getName());
                            }
                        }
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
