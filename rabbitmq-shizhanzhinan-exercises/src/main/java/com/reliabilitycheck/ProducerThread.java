package com.reliabilitycheck;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * ProducerThread:
 *
 * @author sunchen
 * @date 2021/9/25 9:05 下午
 */
public class ProducerThread implements Runnable {
    private final HashMap<String, Integer> map;
    private final Connection connection;

    public ProducerThread(HashMap<String, Integer> map, Connection connection) {
        this.map = map;
        this.connection = connection;
    }

    @SneakyThrows
    @Override
    public void run() {
        // 1. 创建channel
        Channel channel = connection.createChannel();
        channel.addReturnListener((i, s, s1, exchange, basicProperties, bytes) -> System.out.println("send error, exchange:" + exchange + ",message:" + new String(bytes)));
        int count = 0;
        while (true) {
            String message = System.currentTimeMillis() + "-" + count++;
            synchronized (map) {
                // 消息记录到map中
                map.put(message, Check.QUEUE_COUNT);
            }
            // 2. 发消息
            channel.basicPublish(Check.exchangeName, Check.routeKey, new BasicProperties(), message.getBytes(StandardCharsets.UTF_8));
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }
}