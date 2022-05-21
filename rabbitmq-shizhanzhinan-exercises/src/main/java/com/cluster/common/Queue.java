package com.cluster.common;

import lombok.Data;

import java.util.Map;

/**
 * Queue:
 *
 * @author sunchen
 * @date 2021/7/31 11:09 下午
 */
@Data
public class Queue {
    private String name ;
    private String vhost ;
    private Boolean durable ;
    private Boolean auto_delete;
    private Map<String , Object> arguments ;
}