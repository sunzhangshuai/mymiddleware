package com.priority;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP;
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
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Producer {
    static String exchangeName = "exchange:study:priority";

    static String queueName = "queue:study:delay:priority";

    static String routingKey = "key.study.delay.priority";

    static String typeDirect = "direct";

    static Integer maxPriority = 10;

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        // 1. 获取连接
        Connection connection = MqConnect.connection();
        assert connection != null;

        // 2. 获取信道
        Channel channel = connection.createChannel();

        // 3. 声明交换器、声明队列、绑定交换器和队列
        channel.exchangeDeclare(exchangeName, typeDirect, true);
        HashMap<String, Object> param = new HashMap<>(1);
        param.put("x-max-priority", maxPriority);
        channel.queueDeclare(queueName, true, false, false, param);
        channel.queueBind(queueName, exchangeName, routingKey);

        // 4. 发送消息
        int maxNum = 100;
        while (maxNum > 0) {
            int i = new Random().nextInt(maxPriority) + 1;
            String msg = "this is " + i + " level msg";
            channel.basicPublish(exchangeName,
                    routingKey,
                    new AMQP.BasicProperties()
                            .builder()
                            .priority(i)
                            .build(),
                    msg.getBytes(StandardCharsets.UTF_8));
            maxNum--;
        }
    }
}
