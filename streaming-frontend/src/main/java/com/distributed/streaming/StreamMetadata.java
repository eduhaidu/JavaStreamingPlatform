package com.distributed.streaming;

import java.time.LocalDateTime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class StreamMetadata {
    @Id
    private String streamName;
    private boolean isLive;
    private LocalDateTime startTime;

    public StreamMetadata() {
    }

    public StreamMetadata(String streamName, boolean isLive, LocalDateTime startTime) {
        this.streamName = streamName;
        this.isLive = isLive;
        this.startTime = startTime;
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
}
