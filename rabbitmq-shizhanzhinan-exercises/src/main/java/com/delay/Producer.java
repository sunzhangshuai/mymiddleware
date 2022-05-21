package com.delay;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Producer {
    static String originExchangeName = "exchange:study:delay:origin";

    static String originQueueNamePre = "queue:study:delay:origin:";

    static String originRoutingKeyPre = "key.study.delay.origin.";

    static String delayExchangeName = "exchange:study:delay";

    static String delayQueueName = "queue:study:delay";

    static String delayRoutingKey = "key.study.delay";

    static List<Integer> times = new ArrayList<>();

    static {
        times.add(5);
        times.add(10);
        times.add(20);
        times.add(30);
        times.add(60);
    }

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        // 声明死信
        channel.exchangeDeclare(delayExchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        channel.queueDeclare(delayQueueName, true, false, true, null);
        channel.queueBind(delayQueueName, delayExchangeName, delayRoutingKey);

        // 声明源
        channel.exchangeDeclare(originExchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        for (Integer integer : times) {
            HashMap<String, Object> params = new HashMap<>();
            params.put("x-dead-letter-exchange", delayExchangeName);
            params.put("x-dead-letter-routing-key", delayRoutingKey);
            params.put("x-message-ttl", integer * 1000);
            String originQueueName = originQueueNamePre + integer;
            channel.queueDeclare(originQueueName, true, false, true, params);
            channel.queueBind(originQueueName, originExchangeName, originRoutingKeyPre + integer);
        }

        for (Integer integer : times) {
            channel.basicPublish(
                    originExchangeName,
                    originRoutingKeyPre + integer,
                    new BasicProperties().builder()
                            .build(),
                    (integer + "秒后过期").getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
