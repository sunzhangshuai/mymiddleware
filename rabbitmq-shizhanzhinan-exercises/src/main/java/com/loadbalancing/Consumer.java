package com.loadbalancing;

import com.common.MqConnect;
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
 * Consumer
 *
 * @author sunchen
 */
public class Consumer {
    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection("127.0.0.1", 5672, "/study");
        Channel channel = connection.createChannel();

        for (int port : Config.ports) {
            String queueName = Config.queueName + port;
            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                //
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            });
        }

    }
}
