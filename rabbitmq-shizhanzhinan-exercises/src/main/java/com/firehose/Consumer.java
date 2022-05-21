package com.firehose;

import com.common.CommonConsumer;
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
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Consumer {
    /**
     * 消费者tag
     */
    static String consumerTagPre = "consumer:study:firehose:";
    static String consumerTag = consumerTagPre + "consumer";
    static String anotherConsumerTag = consumerTagPre + "consumer.another";
    
    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();
        channel.basicConsume(Producer.queueName, false, consumerTag, new CommonConsumer(channel));
        channel.basicConsume(Producer.anotherQueueName, false, anotherConsumerTag, new CommonConsumer(channel));
    }
}
