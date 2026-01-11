package com.distributed.streaming.dto;

public class AuthResponse {
    
    private String token;
    private String username;
    private String email;
    private String streamKey;
    
    // Constructors
    public AuthResponse() {}
    
    public AuthResponse(String token, String username, String email, String streamKey) {
        this.token = token;
        this.username = username;
        this.email = email;
        this.streamKey = streamKey;
    }
    
    // Getters and Setters
    public String getToken() {
        return token;
    }
    
    public void setToken(String token) {
        this.token = token;
    }
    
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
    
    public String getStreamKey() {
        return streamKey;
    }
    
    public void setStreamKey(String streamKey) {
        this.streamKey = streamKey;
    }
}
