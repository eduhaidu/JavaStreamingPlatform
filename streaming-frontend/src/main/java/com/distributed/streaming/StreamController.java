package com.distributed.streaming;

import java.io.InputStream;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class StreamController {
    private final MinioClient minioClient;
    private final String BUCKET_NAME = "video-storage";

    @Autowired
    private StreamRepository streamRepository;
    
    @Autowired
    private VodRepository vodRepository;

    public StreamController() {
        // Initialize MinIO client
        this.minioClient = MinioClient.builder()
            .endpoint("http://localhost:9000")
            .credentials("minioadmin", "minioadmin123")
            .build();
    }

    @GetMapping("/stream/{filename}")
    public ResponseEntity<InputStreamResource> proxyStream(@PathVariable("filename") String filename) {
        try {
            System.out.println("=== Stream Request ===");
            System.out.println("Fetching from MinIO bucket: " + BUCKET_NAME + "/" + filename);
            
            // Check if object exists and get metadata
            StatObjectResponse stat = minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(filename)
                    .build()
            );
            
            System.out.println("✓ Object found. Size: " + stat.size() + " bytes, ContentType: " + stat.contentType());

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
            headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
            headers.add("Access-Control-Allow-Origin", "*");
            headers.add("Access-Control-Allow-Methods", "GET, OPTIONS");
            headers.add("Access-Control-Allow-Headers", "*");
            headers.add("Access-Control-Expose-Headers", "Content-Length, Content-Type");
            headers.setContentLength(stat.size());

            System.out.println("✓ Streaming " + filename + " (" + mediaType + ")");
            return ResponseEntity.ok()
                .headers(headers)
                .body(new InputStreamResource(stream));
        } catch (Exception e) {
            System.err.println("✗ Error fetching " + filename + " from MinIO: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(500).build();
        }
    }

    @GetMapping("/streams")
    public ResponseEntity<List<StreamMetadata>> getActiveStreams() {
        return ResponseEntity.ok(streamRepository.findByIsLiveTrue());
    }
    
    @GetMapping("/vod")
    public ResponseEntity<List<VodRecording>> getVodRecordings() {
        return ResponseEntity.ok(vodRepository.findAllByOrderByRecordedAtDesc());
    }
    
    @GetMapping("/vod/{vodPath}/{filename}")
    public ResponseEntity<InputStreamResource> proxyVod(
            @PathVariable("vodPath") String vodPath, 
            @PathVariable("filename") String filename) {
        try {
            String objectPath = "vod/" + vodPath + "/" + filename;
            System.out.println("=== VOD Request ===");
            System.out.println("Fetching VOD from MinIO: " + BUCKET_NAME + "/" + objectPath);
            
            StatObjectResponse stat = minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(objectPath)
                    .build()
            );
            
            System.out.println("✓ VOD found. Size: " + stat.size() + " bytes");

            InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(objectPath)
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
            headers.add("Cache-Control", "public, max-age=31536000"); // Cache VOD for 1 year
            headers.add("Access-Control-Allow-Origin", "*");
            headers.setContentLength(stat.size());

            System.out.println("✓ Streaming VOD: " + filename);
            return ResponseEntity.ok()
                .headers(headers)
                .body(new InputStreamResource(stream));
        } catch (Exception e) {
            System.err.println("✗ Error fetching VOD: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(500).build();
        }
    }
    
    
}
