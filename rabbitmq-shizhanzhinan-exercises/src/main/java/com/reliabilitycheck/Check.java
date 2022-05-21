package com.reliabilitycheck;

import com.common.MqConnect;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Check:
 *
 * @author sunchen
 * @date 2021/9/25 9:06 下午
 */
public class Check {
    public static String exchangeName = "exchange:study:reliability";
    public static String queue1 = "queue:study:reliability:1";
    public static String queue2 = "queue:study:reliability:2";
    public static String queue3 = "queue:study:reliability:3";
    public static String routeKey = "key:study:reliability";

    public static int QUEUE_COUNT = 3;
    public static int intervalTime = 1;

    static Connection connection;

    public static void declare() throws IOException, TimeoutException {
        connection = MqConnect.connection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        channel.queueDeclare(queue1, true, false, false, null);
        channel.queueDeclare(queue2, true, false, false, null);
        channel.queueDeclare(queue3, true, false, false, null);
        channel.queueBind(queue1, exchangeName, routeKey);
        channel.queueBind(queue2, exchangeName, routeKey);
        channel.queueBind(queue3, exchangeName, routeKey);
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        declare();
        HashMap<String, Integer> map = new HashMap<>();
        ProducerThread producerThread = new ProducerThread(map, connection);
        ConsumerThread consumerThread1 = new ConsumerThread(map, connection, queue1);
        ConsumerThread consumerThread2 = new ConsumerThread(map, connection, queue2);
        ConsumerThread consumerThread3 = new ConsumerThread(map, connection, queue3);
        DetectThread detectThread = new DetectThread(map);
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(producerThread);
        executorService.submit(consumerThread1);
        executorService.submit(consumerThread2);
        executorService.submit(consumerThread3);
        executorService.submit(detectThread);
        executorService.shutdown();
    }
}