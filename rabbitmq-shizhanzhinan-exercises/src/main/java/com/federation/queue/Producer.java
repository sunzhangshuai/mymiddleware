package com.federation.queue;

import com.common.MqConnect;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * BjProducer:
 *
 * @author sunchen
 * @date 2021/9/11 11:32 下午
 */
public class Producer{
    /**
     * 交换器名称
     */
    static String exchangeName = "exchange:study:federation:exchange";

    /**
     * 上海队列名称
     */
    static String shQueueName = "queue:study:federation:shanghai:queue";

    /**
     * 北京队列名称
     */
    static String bjQueueName = "queue:study:federation:beijing:queue";

    /**
     * 上海绑定键
     */
    static String SHRoutingKey = "key:study:federation:shanghai:queue";

    /**
     * 北京绑定键
     */
    static String BJRoutingKey = "key:study:federation:beijing:queue";

    /**
     * 消息个数
     */
    static int msgNum = 5;

    public static void main(String[] args) throws IOException, TimeoutException {
        // 1. 创建连接
        Connection connection = MqConnect.connection();
        // 2. 获取信道
        Channel channel = connection.createChannel();
        // 3. 定义交换器
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT.getType());
        // 4. 定义上海队列
        channel.queueDeclare(shQueueName, true, false, false, null);
        // 5. 绑定上海队列
        channel.queueBind(shQueueName, exchangeName, SHRoutingKey);

        // 4. 定义北京队列
        channel.queueDeclare(bjQueueName, true, false, false, null);
        // 5. 绑定北京队列
        channel.queueBind(bjQueueName, exchangeName, BJRoutingKey);

        // 6. 发消息
        for (int i = 0; i < msgNum; i++) {
            byte[] bjMsg = ("来自北京的消息：" + i).getBytes(StandardCharsets.UTF_8);
            byte[] shMsg = ("来自上海的消息：" + i).getBytes(StandardCharsets.UTF_8);
            channel.basicPublish(exchangeName, BJRoutingKey, null, bjMsg);
            channel.basicPublish(exchangeName, SHRoutingKey, null, shMsg);
        }
        // 7. 关闭
        channel.close();
        connection.close();
    }
}