package com.reliabilitycheck;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.HashMap;

/**
 * ConsumerThread:
 *
 * @author sunchen
 * @date 2021/9/25 9:45 下午
 */
public class ConsumerThread implements Runnable {
    private final HashMap<String, Integer> map;
    private final Connection connection;
    private final String queueName;

    public ConsumerThread(HashMap<String, Integer> map, Connection connection, String queueName) {
        this.map = map;
        this.connection = connection;
        this.queueName = queueName;
    }

    @SneakyThrows
    @Override
    public void run() {
        Channel channel = connection.createChannel();
        channel.basicQos(64);
        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                // 1.先从map中查看有没有消息
                String message = new String(body);
                synchronized (map) {
                    if (map.containsKey(message)) {
                        int value = map.get(message);
                        value--;
                        if (value > 0) {
                            map.put(message, value);
                        } else {
                            map.remove(message);
                        }
                    } else {
                        System.out.println("message lose");
                    }
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });
    }
}