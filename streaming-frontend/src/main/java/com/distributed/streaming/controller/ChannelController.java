package com.distributed.streaming.controller;

import com.distributed.streaming.StreamMetadata;
import com.distributed.streaming.StreamRepository;
import com.distributed.streaming.VodRecording;
import com.distributed.streaming.VodRepository;
import com.distributed.streaming.dto.ChannelProfileDTO;
import com.distributed.streaming.entity.User;
import com.distributed.streaming.repository.UserRepository;
import com.distributed.streaming.service.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/channels")
@CrossOrigin(origins = "*")
public class ChannelController {
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private StreamRepository streamRepository;
    
    @Autowired
    private VodRepository vodRepository;
    
    @Autowired
    private AuthService authService;
    
    @GetMapping("/{username}")
    public ResponseEntity<?> getChannelProfile(@PathVariable("username") String username) {
        User user = userRepository.findByUsername(username)
                .orElseGet(() -> {
                    Map<String, String> error = new HashMap<>();
                    error.put("error", "Channel not found");
                    return null;
                });
        
        if (user == null) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Channel not found");
            return ResponseEntity.notFound().build();
        }
        
        ChannelProfileDTO profile = new ChannelProfileDTO(
            user.getUsername(),
            user.getEmail(),
            user.getBio(),
            user.getAvatarUrl(),
            user.getCreatedAt()
        );
        
        // Check if user is currently live
        List<StreamMetadata> liveStreams = streamRepository.findByUser_UsernameAndIsLiveTrue(username);
        if (!liveStreams.isEmpty()) {
            profile.setLive(true);
            profile.setCurrentStreamName(liveStreams.get(0).getStreamName());
        } else {
            profile.setLive(false);
        }
        
        return ResponseEntity.ok(profile);
    }
    
    @GetMapping("/{username}/streams")
    public ResponseEntity<List<StreamMetadata>> getChannelStreams(@PathVariable("username") String username) {
        List<StreamMetadata> streams = streamRepository.findByUser_UsernameAndIsLiveTrue(username);
        return ResponseEntity.ok(streams);
    }
    
    @GetMapping("/{username}/vods")
    public ResponseEntity<List<VodRecording>> getChannelVods(@PathVariable("username") String username) {
        List<VodRecording> vods = vodRepository.findByUser_UsernameOrderByRecordedAtDesc(username);
        return ResponseEntity.ok(vods);
    }
    
    @PutMapping("/{username}/profile")
    public ResponseEntity<?> updateProfile(
            @PathVariable("username") String username,
            @RequestHeader("Authorization") String authHeader,
            @RequestBody Map<String, String> updates) {
        
        // Validate JWT and ensure user can only update their own profile
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return ResponseEntity.status(401).body(Map.of("error", "Unauthorized"));
        }
        
        String token = authHeader.substring(7);
        if (!authService.validateToken(token)) {
            return ResponseEntity.status(401).body(Map.of("error", "Invalid token"));
        }
        
        String loggedInUsername = authService.getUsernameFromToken(token);
        if (!loggedInUsername.equals(username)) {
            return ResponseEntity.status(403).body(Map.of("error", "Cannot update other user's profile"));
        }
        
        User user = userRepository.findByUsername(username)
                .orElse(null);
        
        if (user == null) {
            return ResponseEntity.notFound().build();
        }
        
        // Update bio if provided
        if (updates.containsKey("bio")) {
            user.setBio(updates.get("bio"));
        }
        
        // Update avatar URL if provided
        if (updates.containsKey("avatarUrl")) {
            user.setAvatarUrl(updates.get("avatarUrl"));
        }
        
        // Update username if provided
        if (updates.containsKey("username")) {
            String newUsername = updates.get("username");
            if (!newUsername.equals(username) && userRepository.existsByUsername(newUsername)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Username already taken"));
            }
            user.setUsername(newUsername);
        }
        
        userRepository.save(user);
        
        return ResponseEntity.ok(Map.of("message", "Profile updated successfully"));
    }
}
