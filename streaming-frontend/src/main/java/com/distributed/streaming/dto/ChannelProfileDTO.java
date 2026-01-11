package com.distributed.streaming.dto;

import java.time.LocalDateTime;

public class ChannelProfileDTO {
    
    private String username;
    private String email;
    private String bio;
    private String avatarUrl;
    private LocalDateTime createdAt;
    private boolean isLive;
    private String currentStreamName;
    
    public ChannelProfileDTO() {}
    
    public ChannelProfileDTO(String username, String email, String bio, String avatarUrl, LocalDateTime createdAt) {
        this.username = username;
        this.email = email;
        this.bio = bio;
        this.avatarUrl = avatarUrl;
        this.createdAt = createdAt;
    }
    
    // Getters and Setters
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getBio() {
        return bio;
    }
    
    public void setBio(String bio) {
        this.bio = bio;
    }
    
    public String getAvatarUrl() {
        return avatarUrl;
    }
    
    public void setAvatarUrl(String avatarUrl) {
        this.avatarUrl = avatarUrl;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public boolean isLive() {
        return isLive;
    }
    
    public void setLive(boolean live) {
        isLive = live;
    }
    
    public String getCurrentStreamName() {
        return currentStreamName;
    }
    
    public void setCurrentStreamName(String currentStreamName) {
        this.currentStreamName = currentStreamName;
    }
}
