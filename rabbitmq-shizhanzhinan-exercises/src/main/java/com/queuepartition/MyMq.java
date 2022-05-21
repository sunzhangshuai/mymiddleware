package com.queuepartition;

import com.common.MqConnect;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Base:
 *
 * @author sunchen
 * @date 2021/9/11 9:32 下午
 */
public class MyMq {
    /**
     * 队列分区数
     */
    static int partNum = 4;

    /**
     * 连接类
     */
    static class Connection {
        /**
         * 连接
         */
        com.rabbitmq.client.Connection connection;

        /**
         * 构造函数
         *
         * @param connection com.rabbitmq.client.Connection 连接
         */
        public Connection(com.rabbitmq.client.Connection connection) {
            this.connection = connection;
        }

        /**
         * 创建信道
         *
         * @return Channel 信道
         * @throws IOException ...
         */
        public Channel createChannel() throws IOException {
            return new Channel(connection.createChannel());
        }

        /**
         * 连接关闭
         *
         * @throws IOException ...
         */
        public void close() throws IOException {
            connection.close();
        }
    }

    /**
     * 信道类
     */
    static class Channel {
        /**
         * 信道
         */
        com.rabbitmq.client.Channel channel;

        /**
         * 构造函数
         *
         * @param channel com.rabbitmq.client.Channel 信道
         */
        public Channel(com.rabbitmq.client.Channel channel) {
            this.channel = channel;
        }

        /**
         * 定义交换器
         *
         * @param exchangeName String 交换器名称
         * @param type         String 交换器类型
         * @throws IOException ...
         */
        public void exchangeDeclare(String exchangeName, String type) throws IOException {
            channel.exchangeDeclare(exchangeName, type);
        }

        /**
         * 定义队列
         *
         * @param queueNamePre String 队列前缀
         * @param durable      boolean 是否持久化
         * @param exclusive    boolean 是否排他
         * @param autoDelete   boolean 是否自动删除
         * @param arguments    Map 格式化参数
         * @throws IOException ...
         */
        public void queueDeclare(String queueNamePre, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            for (int i = 0; i < partNum; i++) {
                channel.queueDeclare(queueNamePre + ":" + i, durable, exclusive, autoDelete, arguments);
            }
        }

        /**
         * 绑定队列
         *
         * @param queueNamePre  String 队列名称前缀
         * @param exchangeName  String 交换器名称
         * @param routingKeyPre String 绑定键名称前缀
         * @throws IOException ...
         */
        public void queueBind(String queueNamePre, String exchangeName, String routingKeyPre) throws IOException {
            for (int i = 0; i < partNum; i++) {
                channel.queueBind(queueNamePre + ":" + i, exchangeName, routingKeyPre + ":" + i);
            }
        }

        /**
         * 发布消息
         *
         * @param exchangeName    String 交换器名称
         * @param routeKey        String 路由键
         * @param basicProperties BasicProperties 参数
         * @param msgByte         byte[] 消息
         * @throws IOException ...
         */
        public void basicPublish(String exchangeName, String routeKey, BasicProperties basicProperties, byte[] msgByte) throws IOException {
            int part = new Random().nextInt(partNum);
            channel.basicPublish(exchangeName, routeKey + ":" + part, basicProperties, msgByte);
        }

        /**
         * 消费消息
         * @param queueNamePre String 队列名称
         * @param autoAck boolean 是否自动确认
         * @param consumerTagPre String 消费者标签
         * @throws IOException ...
         */
        public void basicConsume(String queueNamePre, boolean autoAck, String consumerTagPre) throws IOException {
            for (int i = 0; i < partNum; i++) {
                channel.basicConsume(queueNamePre + ":" + i, autoAck, consumerTagPre + ":" + i, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                        System.out.println(new String(body));
                        channel.basicAck(envelope.getDeliveryTag(), true);
                    }
                });
            }
        }

        /**
         * 信道关闭
         *
         * @throws IOException      ...
         * @throws TimeoutException ...
         */
        public void close() throws IOException, TimeoutException {
            channel.close();
        }
    }

    /**
     * 获取连接
     *
     * @return Connection 连接
     * @throws IOException      ...
     * @throws TimeoutException ...
     */
    public Connection connection() throws IOException, TimeoutException {
        return new Connection(MqConnect.connection());
    }
}