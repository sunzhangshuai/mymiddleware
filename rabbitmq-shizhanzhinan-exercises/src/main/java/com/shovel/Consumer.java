package com.shovel;

import com.common.MqConnect;
import com.shovel.Producer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Consumer {
    /**
     * 消费者tag
     */
    static String consumerTag = "consumer:study:source:shovel";

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();

        Channel channel = connection.createChannel();
        channel.basicConsume(Producer.sourceQueueName, false, consumerTag, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                System.out.println("消息内容：" + new String(body));
                channel.basicAck(envelope.getDeliveryTag(), true);
            }
        });
    }
}
