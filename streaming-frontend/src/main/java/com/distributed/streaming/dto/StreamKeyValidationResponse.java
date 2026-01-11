package com.distributed.streaming.dto;

public class StreamKeyValidationResponse {
    private boolean valid;
    private Long userId;
    private String username;
    private String message;

    public StreamKeyValidationResponse() {
    }

    public StreamKeyValidationResponse(boolean valid, Long userId, String username, String message) {
        this.valid = valid;
        this.userId = userId;
        this.username = username;
        this.message = message;
    }

    // Getters and Setters
    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
