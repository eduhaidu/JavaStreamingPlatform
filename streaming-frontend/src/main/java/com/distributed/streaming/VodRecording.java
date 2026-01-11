package com.distributed.streaming;

import java.time.LocalDateTime;

import com.distributed.streaming.entity.User;
import jakarta.persistence.*;

@Entity
public class VodRecording {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String streamName;
    private String vodPath;  // Path in MinIO: vod/streamName_timestamp/
    private LocalDateTime recordedAt;
    private Long duration;   // Duration in seconds (optional)
    private Long fileSize;   // Total size in bytes (optional)
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    private User user;
    
    public VodRecording() {}
    
    public VodRecording(String streamName, String vodPath, LocalDateTime recordedAt) {
        this.streamName = streamName;
        this.vodPath = vodPath;
        this.recordedAt = recordedAt;
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public String getVodPath() {
        return vodPath;
    }

    public void setVodPath(String vodPath) {
        this.vodPath = vodPath;
    }

    public LocalDateTime getRecordedAt() {
        return recordedAt;
    }

    public void setRecordedAt(LocalDateTime recordedAt) {
        this.recordedAt = recordedAt;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }
    
    public User getUser() {
        return user;
    }
    
    public void setUser(User user) {
        this.user = user;
    }
}
