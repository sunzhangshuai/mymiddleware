package com.cluster.monitor;


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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class Producer {
    /**
     * 交换器
     */
    static String exchangeName = "exchange:study:cluster:monitor";

    /**
     * 队列前缀
     */
    static String queuePre = "queue:study:cluster:monitor:";

    /**
     * 绑定键
     */
    static String routingKey = "key.study.cluster:monitor";

    /**
     * 消息发送总数
     */
    static int msgTotalCount = 100000;

    /**
     * 端口列表
     */
    static List<Integer> ports = new ArrayList<Integer>() {
        {
            add(5672);
            add(5673);
            add(5674);
        }
    };

    /**
     * 链接
     */
    static List<Connection> connList = new ArrayList<>();

    /**
     * 信道
     */
    static List<Channel> channelList = new ArrayList<>();

    /**
     * 获取信道列表
     *
     * @throws IOException      ...
     * @throws TimeoutException ...
     */
    public static void createChannelList() throws IOException, TimeoutException {
        for (Integer port : ports) {
            Connection connection = MqConnect.connection(MqConnect.ip, port, MqConnect.virtualHost);
            Channel channel = connection.createChannel();
            channelList.add(channel);
            connList.add(connection);
        }
    }

    /**
     * 随机选取一个信道
     *
     * @return 信道
     */
    public static Channel getChannel() {
        Collections.shuffle(channelList);
        return channelList.get(0);
    }

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        createChannelList();
        // 声明交换器
        getChannel().exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, true, null);
        for (int i = 1; i <= ports.size(); i++) {
            Channel channel = getChannel();
            channel.queueDeclare(queuePre + i, true, false, true, null);
            channel.queueBind(queuePre + i, exchangeName, routingKey);
        }
        // 发消息
        for (int i = 0; i < msgTotalCount; i++) {
            getChannel().basicPublish(exchangeName,
                    routingKey,
                    new BasicProperties()
                            .builder()
                            .deliveryMode(2)
                            .build(),
                    "laopo,i love you".getBytes(StandardCharsets.UTF_8));
        }
        channelList.forEach(item -> {
            try {
                item.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        connList.forEach(item -> {
            try {
                item.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
