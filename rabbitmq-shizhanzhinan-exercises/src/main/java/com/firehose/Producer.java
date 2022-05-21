package com.firehose;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Producer {
    /**
     * 交换器名称
     */
    public static String traceExchangeName = "amq.rabbitmq.trace";
    public static String exchangeNamePre = "exchange:study:firehose:";
    public static String exchangeName = exchangeNamePre + "exchange";
    public static String anotherExchangeName = exchangeNamePre + "exchange.another";

    /**
     * 队列名称
     */
    public static String queueNamePre = "queue:study:firehose:";
    public static String queueName = queueNamePre + "queue";
    public static String anotherQueueName = queueNamePre + "queue.another";
    public static String tracePublishAllQueueName = queueNamePre + "trace:publish:all";
    public static String traceDeliverAllQueueName = queueNamePre + "trace:deliver:all";
    public static String traceAllQueueName = queueNamePre + "trace:all";
    public static String tracePublishExchangeQueueName = queueNamePre + "trace:publish:exchange";
    public static String traceDeliverQueueQueueName = queueNamePre + "trace:deliver:queue";

    /**
     * 绑定键
     */
    static String routingKeyPre = "key.study.firehose:";
    static String routingKey = routingKeyPre + "rk";
    static String anotherRoutingKey = routingKeyPre + "rk.another";
    static String tracePublishAllRoutingKey = "publish.#";
    static String traceDeliverAllRoutingKey = "deliver.#";
    static String traceAllRoutingKey = "#";
    static String tracePublishExchangeRoutingKey = "publish." + exchangeName;
    static String traceDeliverQueueRoutingKey = "deliver." + queueName;

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();

        // 声明交换器
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        channel.exchangeDeclare(anotherExchangeName, BuiltinExchangeType.DIRECT, true, true, null);

        // 声明队列
        channel.queueDeclare(queueName, true, false, true, null);
        channel.queueDeclare(anotherQueueName, true, false, true, null);
        channel.queueDeclare(tracePublishAllQueueName, true, false, true, null);
        channel.queueDeclare(traceDeliverAllQueueName, true, false, true, null);
        channel.queueDeclare(traceAllQueueName, true, false, true, null);
        channel.queueDeclare(tracePublishExchangeQueueName, true, false, true, null);
        channel.queueDeclare(traceDeliverQueueQueueName, true, false, true, null);

        // 交换器绑定队列
        channel.queueBind(queueName, exchangeName, routingKey);
        channel.queueBind(anotherQueueName, anotherExchangeName, anotherRoutingKey);
        channel.queueBind(tracePublishAllQueueName, traceExchangeName, tracePublishAllRoutingKey);
        channel.queueBind(traceDeliverAllQueueName, traceExchangeName, traceDeliverAllRoutingKey);
        channel.queueBind(traceAllQueueName, traceExchangeName, traceAllRoutingKey);
        channel.queueBind(tracePublishExchangeQueueName, traceExchangeName, tracePublishExchangeRoutingKey);
        channel.queueBind(traceDeliverQueueQueueName, traceExchangeName, traceDeliverQueueRoutingKey);

        // 发消息
        channel.basicPublish(exchangeName,
                routingKey,
                new BasicProperties()
                        .builder()
                        .deliveryMode(2)
                        .build(),
                "laopo,i love you".getBytes(StandardCharsets.UTF_8));

        channel.basicPublish(anotherExchangeName,
                anotherRoutingKey,
                new BasicProperties()
                        .builder()
                        .deliveryMode(2)
                        .build(),
                "laopo,i love you".getBytes(StandardCharsets.UTF_8));

        channel.close();
        connection.close();
    }
}
