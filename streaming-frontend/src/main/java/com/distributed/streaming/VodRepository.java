package com.distributed.streaming;

import java.util.List;

import com.distributed.streaming.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface VodRepository extends JpaRepository<VodRecording, Long> {
    List<VodRecording> findByStreamName(String streamName);
    List<VodRecording> findAllByOrderByRecordedAtDesc();
    List<VodRecording> findByUserOrderByRecordedAtDesc(User user);
    List<VodRecording> findByUser_UsernameOrderByRecordedAtDesc(String username);
}
