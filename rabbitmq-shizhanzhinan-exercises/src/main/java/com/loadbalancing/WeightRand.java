package com.loadbalancing;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Random:
 *
 * @author sunchen
 * @date 2021/9/28 10:55 下午
 */
public class WeightRand {
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
        Random random = new Random();
        int index = random.nextInt(ports.size());
        port = ports.get(index);
        connection = MqConnect.connection("127.0.0.1", port, "/study");
        return connection.createChannel();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        initPorts();
        for (int i = 0; i < Config.messageCount; i++) {
            Channel channel = getChannel();
            Config.declare(channel, port);
            Config.publish(channel, "this is random:" + i, port);
            channel.close();
            connection.close();
        }
    }
}