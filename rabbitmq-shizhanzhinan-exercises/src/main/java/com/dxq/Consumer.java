package com.dxq;

import com.common.MqConnect;
import com.common.CommonConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.Arrays;
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
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        channel.basicConsume(Producer.originQueueName, false, consumerTag, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                channel.basicReject(envelope.getDeliveryTag(), false);
            }
        });
    }
}
