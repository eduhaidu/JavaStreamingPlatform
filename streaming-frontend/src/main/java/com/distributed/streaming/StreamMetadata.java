package com.distributed.streaming;

import java.time.LocalDateTime;

import com.distributed.streaming.entity.User;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

@Entity
public class StreamMetadata {
    @Id
    private String streamName;
    private boolean isLive;
    private LocalDateTime startTime;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    private User user;
    
    private String streamTitle;
    private Integer viewerCount;

    public StreamMetadata() {
    }

    public StreamMetadata(String streamName, boolean isLive, LocalDateTime startTime) {
        this.streamName = streamName;
        this.isLive = isLive;
        this.startTime = startTime;
        this.viewerCount = 0;
    }

    public String getStreamName() {
        return streamName;
    }
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }
    public boolean isLive() {
        return isLive;
    }
    public void setLive(boolean isLive) {
        this.isLive = isLive;
    }
    public LocalDateTime getStartTime() {
        return startTime;
    }
    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }
    
    public User getUser() {
        return user;
    }
    
    public void setUser(User user) {
        this.user = user;
    }
    
    public String getStreamTitle() {
        return streamTitle;
    }
    
    public void setStreamTitle(String streamTitle) {
        this.streamTitle = streamTitle;
    }
    
    public Integer getViewerCount() {
        return viewerCount;
    }
    
    public void setViewerCount(Integer viewerCount) {
        this.viewerCount = viewerCount;
    }
}
