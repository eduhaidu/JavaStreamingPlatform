package com.distributed.streaming.controller;

import com.distributed.streaming.entity.User;
import com.distributed.streaming.repository.UserRepository;
import com.distributed.streaming.service.AuthService;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.StatObjectArgs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/upload")
@CrossOrigin(origins = "*")
public class UploadController {
    
    private final MinioClient minioClient;
    private final String BUCKET_NAME = "video-storage";
    
    @Autowired
    private AuthService authService;
    
    @Autowired
    private UserRepository userRepository;
    
    public UploadController() {
        this.minioClient = MinioClient.builder()
            .endpoint("http://localhost:9000")
            .credentials("minioadmin", "minioadmin123")
            .build();
    }
    
    @PostMapping("/avatar")
    public ResponseEntity<?> uploadAvatar(
            @RequestParam("file") MultipartFile file,
            @RequestHeader("Authorization") String authHeader) {
        
        try {
            // Validate JWT
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                return ResponseEntity.status(401).body(Map.of("error", "Unauthorized"));
            }
            
            String token = authHeader.substring(7);
            if (!authService.validateToken(token)) {
                return ResponseEntity.status(401).body(Map.of("error", "Invalid token"));
            }
            
            String username = authService.getUsernameFromToken(token);
            User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new RuntimeException("User not found"));
            
            // Validate file
            if (file.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "No file provided"));
            }
            
            // Check file size (max 5MB)
            if (file.getSize() > 5 * 1024 * 1024) {
                return ResponseEntity.badRequest().body(Map.of("error", "File size must be less than 5MB"));
            }
            
            // Check file type
            String contentType = file.getContentType();
            if (contentType == null || !contentType.startsWith("image/")) {
                return ResponseEntity.badRequest().body(Map.of("error", "Only image files are allowed"));
            }
            
            // Generate unique filename
            String originalFilename = file.getOriginalFilename();
            String extension = "";
            if (originalFilename != null && originalFilename.contains(".")) {
                extension = originalFilename.substring(originalFilename.lastIndexOf("."));
            }
            String filename = "avatars/" + user.getId() + "_" + UUID.randomUUID() + extension;
            
            // Upload to MinIO
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(filename)
                    .stream(file.getInputStream(), file.getSize(), -1)
                    .contentType(contentType)
                    .build()
            );
            
            // Generate URL
            String avatarUrl = "/api/avatars/" + filename.substring("avatars/".length());
            
            // Update user's avatar URL
            user.setAvatarUrl(avatarUrl);
            userRepository.save(user);
            
            Map<String, String> response = new HashMap<>();
            response.put("url", avatarUrl);
            response.put("message", "Avatar uploaded successfully");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body(Map.of("error", "Failed to upload avatar: " + e.getMessage()));
        }
    }
    
    @GetMapping("/avatars/{filename}")
    public ResponseEntity<InputStreamResource> getAvatar(@PathVariable("filename") String filename) {
        try {
            String objectPath = "avatars/" + filename;
            
            // Get object metadata
            var stat = minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(objectPath)
                    .build()
            );
            
            // Get object stream
            InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(objectPath)
                    .build()
            );
            
            // Determine content type
            MediaType mediaType = MediaType.APPLICATION_OCTET_STREAM;
            if (stat.contentType() != null && stat.contentType().startsWith("image/")) {
                mediaType = MediaType.parseMediaType(stat.contentType());
            }
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(mediaType);
            headers.setContentLength(stat.size());
            headers.add("Cache-Control", "public, max-age=31536000"); // Cache for 1 year
            headers.add("Access-Control-Allow-Origin", "*");
            
            return ResponseEntity.ok()
                .headers(headers)
                .body(new InputStreamResource(stream));
                
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.notFound().build();
        }
    }
}
