package com.distributed.streaming;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.distributed.streaming.entity.User;

/**
 *
 * @author eduhaidu
 */
public interface StreamRepository extends JpaRepository<StreamMetadata, String>{
    List<StreamMetadata> findByIsLiveTrue();
    List<StreamMetadata> findByUserAndIsLiveTrue(User user);
    List<StreamMetadata> findByUser_UsernameAndIsLiveTrue(String username);
    StreamMetadata findByStreamName(String streamName);
}
