package com.distributed.streaming.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.distributed.streaming.VodRecording;

@Repository
public interface VodRecordingRepository extends JpaRepository<VodRecording, Long> {
    List<VodRecording> findByUser_IdOrderByRecordedAtDesc(Long userId);
    List<VodRecording> findAllByOrderByRecordedAtDesc();
}
