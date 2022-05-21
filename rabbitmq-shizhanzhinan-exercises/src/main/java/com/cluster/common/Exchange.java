package com.cluster.common;

import lombok.Data;

import java.util.Map;

/**
 * Exchange:
 *
 * @author sunchen
 * @date 2021/7/31 11:09 下午
 */
@Data
public class Exchange {
    private String name;
    private String vhost ;
    private String type ;
    private Boolean durable ;
    private Boolean auto_delete ;
    private Boolean internal;
    private Map<String , Object> arguments;
}