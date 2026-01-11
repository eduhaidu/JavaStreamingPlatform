package com.distributed.streaming.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.distributed.streaming.dto.StreamKeyValidationResponse;
import com.distributed.streaming.entity.User;
import com.distributed.streaming.repository.UserRepository;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/stream")
@CrossOrigin(origins = "*")
public class StreamKeyController {

    @Autowired
    private UserRepository userRepository;

    /**
     * Validate stream key and return user information
     * This endpoint will be called by Red5 server before allowing RTMP connection
     */
    @GetMapping("/validate-key/{streamKey}")
    public ResponseEntity<StreamKeyValidationResponse> validateStreamKey(@PathVariable String streamKey) {
        try {
            User user = userRepository.findByStreamKey(streamKey).orElse(null);
            
            if (user != null) {
                StreamKeyValidationResponse response = new StreamKeyValidationResponse();
                response.setValid(true);
                response.setUserId(user.getId());
                response.setUsername(user.getUsername());
                response.setMessage("Stream key is valid");
                return ResponseEntity.ok(response);
            } else {
                StreamKeyValidationResponse response = new StreamKeyValidationResponse();
                response.setValid(false);
                response.setMessage("Invalid stream key");
                return ResponseEntity.ok(response);
            }
        } catch (Exception e) {
            StreamKeyValidationResponse response = new StreamKeyValidationResponse();
            response.setValid(false);
            response.setMessage("Error validating stream key: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    /**
     * Quick endpoint to check if stream key exists (for Red5 plugin)
     */
    @GetMapping("/check-key/{streamKey}")
    public ResponseEntity<Map<String, Object>> checkStreamKey(@PathVariable String streamKey) {
        Map<String, Object> response = new HashMap<>();
        User user = userRepository.findByStreamKey(streamKey).orElse(null);
        
        if (user != null) {
            response.put("valid", true);
            response.put("userId", user.getId());
            response.put("username", user.getUsername());
        } else {
            response.put("valid", false);
        }
        
        return ResponseEntity.ok(response);
    }
}
