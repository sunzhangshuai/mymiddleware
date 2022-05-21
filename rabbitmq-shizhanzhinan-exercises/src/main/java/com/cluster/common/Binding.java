package com.cluster.common;

import lombok.Data;

import java.util.Map;

/**
 * Binding:
 *
 * @author sunchen
 * @date 2021/7/31 11:09 下午
 */
@Data
public class Binding {
    private String source ;
    private String vhost ;
    private String destination ;
    private String destination_type ;
    private String routing_key ;
    private Map<String, Object> arguments ;
}