package com.loadbalancing;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * RoundRobin:
 *
 * @author sunchen
 * @date 2021/9/28 9:59 下午
 */
public class RoundRobin {
    static int index = 0;
    static int port = 0;
    static Connection connection;

    /**
     * 获取信道
     *
     * @return Channel 信道
     * @throws IOException      ...
     * @throws TimeoutException ...
     */
    public static Channel getChannel() throws IOException, TimeoutException {
        port = Config.ports.get(index);
        index = (index + 1) % Config.ports.size();
        connection = MqConnect.connection("127.0.0.1", port, "/study");
        return connection.createChannel();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        for (int i = 0; i < Config.messageCount; i++) {
            Channel channel = getChannel();
            Config.declare(channel, port);
            Config.publish(channel, "this is roundRobin:" + i, port);
            channel.close();
            connection.close();
        }
    }
}