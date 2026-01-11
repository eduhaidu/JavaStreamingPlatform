package com.distributed.streaming.dto;

import java.time.LocalDateTime;

public class StreamPreviewDTO {
    private String streamName;
    private String streamTitle;
    private String username;
    private String avatarUrl;
    private Integer viewerCount;
    private LocalDateTime startTime;
    private boolean isLive;

    public StreamPreviewDTO() {
    }

    public StreamPreviewDTO(String streamName, String streamTitle, String username, 
                           String avatarUrl, Integer viewerCount, LocalDateTime startTime, boolean isLive) {
        this.streamName = streamName;
        this.streamTitle = streamTitle;
        this.username = username;
        this.avatarUrl = avatarUrl;
        this.viewerCount = viewerCount;
        this.startTime = startTime;
        this.isLive = isLive;
    }

    // Getters and Setters
    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public String getStreamTitle() {
        return streamTitle;
    }

    public void setStreamTitle(String streamTitle) {
        this.streamTitle = streamTitle;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getAvatarUrl() {
        return avatarUrl;
    }

    public void setAvatarUrl(String avatarUrl) {
        this.avatarUrl = avatarUrl;
    }

    public Integer getViewerCount() {
        return viewerCount;
    }

    public void setViewerCount(Integer viewerCount) {
        this.viewerCount = viewerCount;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public boolean isLive() {
        return isLive;
    }

    public void setLive(boolean live) {
        isLive = live;
    }
}
