package com.dxq;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Producer {
    static String originExchangeName = "exchange:study:dx:origin";

    static String originQueueName = "queue:study:dx:origin";

    static String originRoutingKey = "key.study.dx.origin";

    static String dxExchangeName = "exchange:study:dx";

    static String dxQueueName = "queue:study:dx";

    static String dxRoutingKey = "key.study.dx";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        // 声明死信
        channel.exchangeDeclare(dxExchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        channel.queueDeclare(dxQueueName, true, false, true, null);
        channel.queueBind(dxQueueName, dxExchangeName, dxRoutingKey);

        // 声明源
        channel.exchangeDeclare(originExchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        Map<String, Object> params = new HashMap<>();
        params.put("x-dead-letter-exchange", dxExchangeName);
        params.put("x-dead-letter-routing-key", dxRoutingKey);
        params.put("x-max-length", 10);
        channel.queueDeclare(originQueueName, true, false, true, params);
        channel.queueBind(originQueueName, originExchangeName, originRoutingKey);

        // 1. 消息过期
        channel.basicPublish(
                originExchangeName,
                originRoutingKey,
                new BasicProperties()
                        .builder()
                        .expiration("1000")
                        .build(),
                "消息过期".getBytes(StandardCharsets.UTF_8));

        // 2. 被拒绝
        channel.basicPublish(
                originExchangeName,
                originRoutingKey,
                new BasicProperties()
                        .builder()
                        .build(),
                "被拒绝".getBytes(StandardCharsets.UTF_8));

        // 3. 队列超长
        for (int i = 0; i < 20; i++) {
            channel.basicPublish(
                    originExchangeName,
                    originRoutingKey,
                    new BasicProperties()
                            .builder()
                            .build(),
                    ("队列超长"+i).getBytes(StandardCharsets.UTF_8));
        }
    }
}
