package com.ae;

import com.common.CommonConsumer;
import com.common.MqConnect;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Consumer {
    static String consumerTag = "consumer:study:dxq";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 连接
        Connection connection = MqConnect.connection();
        assert connection != null;

        // 信道
        Channel channel = connection.createChannel();

        // 推模式开始消费
        channel.basicConsume(Producer.aeQueueName, false, consumerTag, new CommonConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException
            {
                System.out.println(new String(body));
                channel.basicReject(envelope.getDeliveryTag(), false);
            }
        });
    }
}
