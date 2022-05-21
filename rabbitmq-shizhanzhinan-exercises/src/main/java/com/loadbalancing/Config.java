package com.loadbalancing;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Config:
 *
 * @author sunchen
 * @date 2021/9/28 9:47 下午
 */
public class Config {
    /**
     * 交换器名称
     */
    public static String exchangeName = "exchange:study:balance";

    /**
     * 队列名称
     */
    public static String queueName = "queue:study:balance:";

    /**
     * 绑定键
     */
    static String routingKey = "key.study.balance:";

    /**
     * 消息发送数量
     */
    static int messageCount = 10;

    /**
     * ip地址
     */
    static String ip = "127.0.0.1";

    /**
     * 端口列表
     */
    public static List<Integer> ports = new ArrayList<Integer>() {{
        add(5672);
        add(5673);
        add(5674);
    }};

    /**
     * 端口连接数列表
     */
    static HashMap<Integer, Integer> weightConnMap = new HashMap<Integer, Integer>() {
        {
            put(5672, 0);
            put(5673, 0);
            put(5674, 0);
        }
    };

    /**
     * 权重端口列表
     */
    static HashMap<Integer, Integer> weightPortMap = new HashMap<Integer, Integer>() {
        {
            put(5672, 7);
            put(5673, 1);
            put(5674, 2);
        }
    };

    /**
     * 声明
     *
     * @param channel Channel 信道
     * @param port    int 端口
     * @throws IOException ...
     */
    public static void declare(Channel channel, int port) throws IOException {
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        channel.queueDeclare(queueName + port, true, false, false, null);
        channel.queueBind(queueName + port, exchangeName, routingKey + port);
    }

    /**
     * 发送消息
     *
     * @param channel Channel 信道
     * @param message String 消息体
     * @param port    int 端口
     * @throws IOException ...
     */
    public static void publish(Channel channel, String message, int port) throws IOException {
        channel.basicPublish(exchangeName,
                routingKey + port,
                new BasicProperties()
                        .builder()
                        .deliveryMode(2)
                        .build(),
                message.getBytes(StandardCharsets.UTF_8));
    }
}