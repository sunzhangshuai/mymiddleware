package com.loadbalancing;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * MinConn:
 *
 * @author sunchen
 * @date 2021/9/28 11:29 下午
 */
public class MinConn {
    static int port = 0;
    static Connection connection;
    static List<Integer> ports = new ArrayList<>();


    /**
     * 获取信道
     *
     * @return Channel 信道
     * @throws IOException      ...
     * @throws TimeoutException ...
     */
    public static Channel getChannel() throws IOException, TimeoutException {
        port = getMinPort();
        connection = MqConnect.connection("127.0.0.1", port, "/study");
        Config.weightConnMap.put(port, Config.weightConnMap.get(port) + 1);
        return connection.createChannel();
    }

    public static int getMinPort() {
        int minPort = Config.ports.get(0);
        int min = Config.weightConnMap.get(minPort);
        for (int port : Config.weightConnMap.keySet()) {
            if (Config.weightConnMap.get(port) < min) {
                minPort = port;
            }
        }
        return minPort;
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        for (int i = 0; i < Config.messageCount; i++) {
            Channel channel = getChannel();
            Config.declare(channel, port);
            Config.publish(channel, "this is minConn:" + i, port);
            channel.close();
            connection.close();
        }
    }
}