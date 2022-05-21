package com.ae;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP;
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
    static String originExchangeName = "exchange:study:ae:origin";

    static String originRoutingKey = "key.study.ae.origin";

    static String originTypeDirect = "direct";

    static String aeExchangeName = "exchange:study:ae";

    static String aeQueueName = "queue:study:ae";

    static String aeRoutingKey = "key.study.ae";

    static String aeTypeDirect = "fanout";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        // 1. 获取连接
        Connection connection = MqConnect.connection();
        assert connection != null;

        // 2. 获取信道
        Channel channel = connection.createChannel();

        // 3. 声明备份交换器、声明备份队列、绑定备份信交换器和备份队列
        channel.exchangeDeclare(aeExchangeName, aeTypeDirect, true);
        channel.queueDeclare(aeQueueName, true, false, false, null);
        channel.queueBind(aeQueueName, aeExchangeName, aeRoutingKey);

        // 4. 声明源交换器，不声明队列
        Map<String, Object> param = new HashMap<>(1);
        param.put("alternate-exchange", aeExchangeName);
        channel.exchangeDeclare(originExchangeName, originTypeDirect, true, true, param);

        // 5. 发送消息
        int maxNum = 100;
        while (maxNum > 0) {
            channel.basicPublish(originExchangeName,
                    originRoutingKey,
                    new AMQP.BasicProperties()
                            .builder()
                            .build(),
                    "alternate-exchange msg".getBytes(StandardCharsets.UTF_8));
            maxNum--;
        }
    }
}
