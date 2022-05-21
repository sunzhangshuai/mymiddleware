package com.loadbalancing;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * WeightRoundRobin:
 *
 * @author sunchen
 * @date 2021/9/28 10:23 下午
 */
public class WeightRoundRobin {
    static int index = 0;
    static int port = 0;
    static Connection connection;
    static List<Integer> ports = new ArrayList<>();

    public static void initPorts() {
        HashMap<Integer, Integer> weightPortMap = Config.weightPortMap;
        for (Integer port: weightPortMap.keySet()) {
            for (int i = 0; i < weightPortMap.get(port); i++) {
                ports.add(port);
            }
        }
    }

    /**
     * 获取信道
     *
     * @return Channel 信道
     * @throws IOException      ...
     * @throws TimeoutException ...
     */
    public static Channel getChannel() throws IOException, TimeoutException {
        port = ports.get(index);
        index = (index + 1) % ports.size();
        connection = MqConnect.connection("127.0.0.1", port, "/study");
        return connection.createChannel();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        initPorts();
        for (int i = 0; i < Config.messageCount; i++) {
            Channel channel = getChannel();
            Config.declare(channel, port);
            Config.publish(channel, "this is weightRoundRobin:" + i, port);
            channel.close();
            connection.close();
        }
    }
}