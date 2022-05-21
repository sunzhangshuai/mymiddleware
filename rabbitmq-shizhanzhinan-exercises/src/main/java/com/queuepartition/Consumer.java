package com.queuepartition;

import com.queuepartition.MyMq.Channel;
import com.queuepartition.MyMq.Connection;
import com.rabbitmq.client.AMQP.BasicProperties;
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
     * mq 实例
     */
    static MyMq MqConnect = new MyMq();

    /**
     * tag名称
     */
    static String consumerTag = "consumer:study:queuePartition";

    /**
     * 消费者
     * @param args 参数
     * @throws IOException ...
     * @throws TimeoutException ...
     * @throws NoSuchAlgorithmException ...
     * @throws KeyManagementException ...
     * @throws URISyntaxException ...
     */
    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();
        channel.basicConsume(Producer.queueName, false, consumerTag);
    }
}
