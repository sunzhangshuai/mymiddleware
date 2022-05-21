package com.queuepartition;

import com.queuepartition.MyMq.Channel;
import com.queuepartition.MyMq.Connection;
import com.rabbitmq.client.BuiltinExchangeType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Product:
 *
 * @author sunchen
 * @date 2021/9/11 9:13 下午
 */
public class Producer {
    /**
     * mq 实例
     */
    static MyMq MqConnect = new MyMq();

    /**
     * 交换器名称
     */
    static String exchangeName = "exchange:study:queuePartition";

    /**
     * 队列名称
     */
    static String queueName = "queue:study:queuePartition";

    /**
     * 绑定键
     */
    static String routeKey = "key:study:queuePartition";

    /**
     * 消息个数
     */
    static int msgNum = 100;

    /**
     * 生产者
     * @param args 参数
     * @throws IOException ...
     * @throws TimeoutException ...
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        // 1. 创建连接
        Connection connection = MqConnect.connection();
        // 2. 获取信道
        Channel channel = connection.createChannel();
        // 3. 定义交换器
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT.getType());
        // 4. 定义队列
        channel.queueDeclare(queueName, true, false, false, null);
        // 5. 绑定队列
        channel.queueBind(queueName, exchangeName, routeKey);
        // 6. 发消息
        for (int i = 0; i < msgNum; i++) {
            byte[] msg = ("queuePartition:" + i).getBytes(StandardCharsets.UTF_8);
            channel.basicPublish(exchangeName, routeKey, null, msg);
        }
        // 7. 关闭
        channel.close();
        connection.close();
    }
}