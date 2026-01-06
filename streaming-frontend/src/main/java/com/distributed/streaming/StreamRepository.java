/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */

package com.distributed.streaming;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 *
 * @author eduhaidu
 */
public interface StreamRepository extends JpaRepository<StreamMetadata, String>{
    List<StreamMetadata> findByIsLiveTrue();
}
