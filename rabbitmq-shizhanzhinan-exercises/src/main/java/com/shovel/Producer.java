package com.shovel;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Producer {
    /**
     * 交换器
     */
    static String exchangeName = "exchange:study:shovel";

    /**
     * 源队列
     */
    static String sourceQueueName = "queue:study:source:shovel";

    /**
     * 目标队列
     */
    static String destQueueName = "queue:study:dest:shovel";

    /**
     * 绑定键
     */
    static String routingKey = "key.study.shovel";

    /**
     * 消息总个数
     */
    static int msgTotalCount = 5000000;

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        // 声明交换器
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, true, null);

        // 声明队列
        channel.queueDeclare(sourceQueueName, true, false, true, null);
        channel.queueDeclare(destQueueName, true, false, true, null);

        // 交换器绑定队列
        channel.queueBind(sourceQueueName, exchangeName, routingKey);

        // 发消息
        for (int i = 0; i < msgTotalCount; i++) {
            channel.basicPublish(exchangeName,
                    routingKey,
                    new BasicProperties()
                            .builder()
                            .deliveryMode(2)
                            .build(),
                    "laopo,i love you".getBytes(StandardCharsets.UTF_8));
        }
        channel.close();
        connection.close();
    }
}
