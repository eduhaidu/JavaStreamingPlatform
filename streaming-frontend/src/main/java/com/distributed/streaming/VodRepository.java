package com.distributed.streaming;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface VodRepository extends JpaRepository<VodRecording, Long> {
    List<VodRecording> findByStreamName(String streamName);
    List<VodRecording> findAllByOrderByRecordedAtDesc();
}
