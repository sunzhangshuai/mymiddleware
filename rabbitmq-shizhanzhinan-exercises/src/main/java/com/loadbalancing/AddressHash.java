package com.loadbalancing;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Hash:
 *
 * @author sunchen
 * @date 2021/9/28 11:09 下午
 */
public class AddressHash {
    static int port = 0;
    static int index = 0;

    static Connection connection;

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
        for (Integer p : Config.weightConnMap.keySet()) {
            int count = Config.weightConnMap.get(port);
            if (count < min) {
                minPort = p;
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