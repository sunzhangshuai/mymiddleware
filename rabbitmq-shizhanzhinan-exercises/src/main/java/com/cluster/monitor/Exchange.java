package com.cluster.monitor;

import lombok.Data;
import lombok.ToString;

/**
 * Exchange:
 *
 * @author sunchen
 * @date 2021/9/21 7:32 下午
 */
@Data
@ToString
public class Exchange {
    /**
     * 消息进来的速度
     */
    private double publishInRate;

    /**
     * 消息进来的数量
     */
    private long publishIn;

    /**
     * 消息出去的速度
     */
    private double publishOutRate;

    /**
     * 消息出去的数量
     */
    private long publishOut;
}