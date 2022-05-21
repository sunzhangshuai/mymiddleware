package com.federation.exchange;

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
 * BjConsumer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class BjConsumer {
    /**
     * tag名称
     */
    static String consumerTag = "consumer:study:upstream:federation:beijing:exchange";

    /**
     * 消费者
     *
     * @param args 参数
     * @throws IOException              ...
     * @throws TimeoutException         ...
     * @throws NoSuchAlgorithmException ...
     * @throws KeyManagementException   ...
     * @throws URISyntaxException       ...
     */
    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        channel.basicConsume(BjProducer.queueName, false, consumerTag, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                System.out.println(new String(body));
                channel.basicAck(envelope.getDeliveryTag(), true);
            }
        });
    }
}
