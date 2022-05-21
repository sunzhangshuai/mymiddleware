package com.cluster.monitor;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.ToString;

/**
 * ClusterNode:
 *
 * @author sunchen
 * @date 2021/9/21 6:34 下午
 */
@Data
@ToString
public class ClusterNode {
    /**
     * 磁盘空间
     */
    @JSONField(name = "disk_free")
    private long diskFree;

    /**
     * 磁盘可用空间
     */
    @JSONField(name = "disk_free_limit")
    private long diskFreeLimit;

    /**
     * 句柄使用数
     */
    @JSONField(name = "fd_used")
    private long fdUsed;

    /**
     * 句柄总数
     */
    @JSONField(name = "fd_total")
    private long fdTotal;

    /**
     * 套接字使用数
     */
    @JSONField(name = "sockets_used")
    private long socketsUsed;

    /**
     * 套接字总数
     */
    @JSONField(name = "sockets_total")
    private long socketsTotal;

    /**
     * 内存使用数
     */
    @JSONField(name = "mem_used")
    private long memoryUsed;

    /**
     * 内存可用数
     */
    @JSONField(name = "mem_limit")
    private long memoryLimit;

    /**
     * erlang进程使用数
     */
    @JSONField(name = "proc_used")
    private long procUsed;

    /**
     * erlang进程总数
     */
    @JSONField(name = "proc_total")
    private long procTotal;

    public static void main(String[] args) {
        System.out.println(new ClusterNode());
    }
}