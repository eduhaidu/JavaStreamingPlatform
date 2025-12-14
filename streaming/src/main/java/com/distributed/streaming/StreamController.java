package com.distributed.streaming;

import java.io.InputStream;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;


@RestController
public class StreamController {
    private final MinioClient minioClient;
    private final String BUCKET_NAME = "video-storage";

    public StreamController() {
        // Initialize MinIO client
        this.minioClient = MinioClient.builder()
            .endpoint("http://localhost:9000")
            .credentials("minioadmin", "minioadmin123")
            .build();
    }

    @GetMapping("/stream/{filename}")
    public ResponseEntity<InputStreamResource> proxyStream(@PathVariable String filename) {
        try {
            System.out.println("Fetching from MinIO bucket: " + BUCKET_NAME + "/" + filename);
            
            // Check if object exists and get metadata
            StatObjectResponse stat = minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(filename)
                    .build()
            );
            
            System.out.println("Object found. Size: " + stat.size() + " bytes");

            // Get the object as a stream
            InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(filename)
                    .build()
            );

            MediaType mediaType = MediaType.APPLICATION_OCTET_STREAM;
            if(filename.endsWith(".m3u8")){
                mediaType = MediaType.parseMediaType("application/x-mpegURL");
            }else if(filename.endsWith(".ts")){
                mediaType = MediaType.parseMediaType("video/MP2T");
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(mediaType);
            headers.add("Cache-Control", "no-cache");
            headers.add("Access-Control-Allow-Origin", "*");
            headers.setContentLength(stat.size());

            return ResponseEntity.ok()
                .headers(headers)
                .body(new InputStreamResource(stream));
        } catch (Exception e) {
            System.err.println("Error fetching from MinIO: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.notFound().build();
        }
    }
    
}
