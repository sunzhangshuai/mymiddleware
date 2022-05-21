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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * ProducerBak: 反例，无法实现延时效果
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class ProducerBak {
    static String originExchangeName = "exchange:study:delay:origin";

    static String originQueueName = "queue:study:delay:origin";

    static String originRoutingKey = "key.study.delay.origin";

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
        HashMap<String, Object> params = new HashMap<>(2);
        params.put("x-dead-letter-exchange", delayExchangeName);
        params.put("x-dead-letter-routing-key", delayRoutingKey);
        channel.queueDeclare(originQueueName, true, false, true, params);
        channel.queueBind(originQueueName, originExchangeName, originRoutingKey);

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(times);
            int strTime = times.get(0);
            channel.basicPublish(
                    originExchangeName,
                    originRoutingKey,
                    new BasicProperties().builder()
                            .expiration(String.valueOf(strTime * 1000))
                            .build(),
                    ("第" + i + "条：" + strTime + "秒后过期").getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
