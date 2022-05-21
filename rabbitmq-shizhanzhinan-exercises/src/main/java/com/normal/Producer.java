package com.normal;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
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
    public static String exchangeName = "exchange:study:normal";

    public static String queueName = "queue:study:normal";

    static String routingKey = "key.study.normal";

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        // 声明交换器
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, true, null);

        // 声明队列
        channel.queueDeclare(queueName, true, false, true, null);

        // 交换器绑定队列
        channel.queueBind(queueName, exchangeName, routingKey);

        // 发消息
        for (int i = 0; i < 100000; i++) {
            channel.basicPublish(exchangeName,
                    routingKey,
                    new BasicProperties()
                            .builder()
                            .deliveryMode(2)
                            .build(),
                    "laopo,i love you".getBytes(StandardCharsets.UTF_8));
        }
    }
}
