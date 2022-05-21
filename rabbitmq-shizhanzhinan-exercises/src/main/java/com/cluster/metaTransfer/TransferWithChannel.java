package com.cluster.metaTransfer;

import com.alibaba.fastjson.JSON;
import com.cluster.common.Binding;
import com.cluster.common.Exchange;
import com.cluster.common.Queue;
import com.cluster.common.Vhost;
import com.common.MqConnect;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * TransferWithChannel:
 *
 * @author sunchen
 * @date 2021/7/31 11:05 下午
 */
public class TransferWithChannel extends Transfer {
    /**
     * 信道map key:vhostName value:信道
     */
    static Multimap<String, Channel> channelMap = ArrayListMultimap.create();

    /**
     * 连接列表
     */
    static List<Connection> connectionList = new ArrayList<>();

    /**
     * 获取信道列表
     *
     * @throws IOException      ...
     * @throws TimeoutException ...
     */
    public static void createChannelList() throws IOException, TimeoutException {
        for (Object obj : vhosts) {
            Vhost vhost = JSON.parseObject(JSON.toJSONString(obj), Vhost.class);
            for (Integer port : ports) {
                String vhostName = vhost.getName();
                Connection connection = MqConnect.connection(ip, port, vhostName);
                Channel channel = connection.createChannel();
                channelMap.put(vhostName, channel);
                connectionList.add(connection);
            }
        }
    }

    /**
     * 随机选取一个信道
     *
     * @param vhostName 虚拟主机名称
     * @return 信道
     */
    public static Channel getChannel(String vhostName) {
        List<Channel> channels = (List<Channel>) channelMap.get(vhostName);
        Collections.shuffle(channels);
        return channels.get(0);
    }

    /**
     * 创建队列
     */
    public static void creteQueues() throws IOException {
        for (Object obj : queueList) {
            Queue queue = JSON.parseObject(JSON.toJSONString(obj), Queue.class);
            Channel channel = getChannel(queue.getVhost());
            channel.queueDeclare(
                    queue.getName(),
                    queue.getDurable(),
                    false,
                    queue.getAuto_delete(),
                    queue.getArguments()
            );
        }
    }

    /**
     * 创建交换器
     */
    public static void createExchanges() throws IOException {
        for (Object obj : exchangeList) {
            Exchange exchange = JSON.parseObject(JSON.toJSONString(obj), Exchange.class);
            Channel channel = getChannel(exchange.getVhost());
            channel.exchangeDeclare(
                    exchange.getName(),
                    exchange.getType(),
                    exchange.getDurable(),
                    exchange.getAuto_delete(),
                    exchange.getInternal(),
                    exchange.getArguments()
            );
        }
    }

    /**
     * 创建绑定键
     */
    public static void createBindings() throws IOException {
        for (Object obj : bindingList) {
            Binding binding = JSON.parseObject(JSON.toJSONString(obj), Binding.class);
            Channel channel = getChannel(binding.getVhost());
            channel.queueBind(
                    binding.getDestination(),
                    binding.getSource(),
                    binding.getRouting_key(),
                    binding.getArguments()
            );
        }
    }

    /**
     * 删除
     */
    public static void delete() throws IOException {
        for (Object obj : queueList) {
            Queue queue = JSON.parseObject(JSON.toJSONString(obj), Queue.class);
            Channel channel = getChannel(queue.getVhost());
            channel.queueDelete(queue.getName());
        }

        for (Object obj : exchangeList) {
            Exchange exchange = JSON.parseObject(JSON.toJSONString(obj), Exchange.class);
            Channel channel = getChannel(exchange.getVhost());
            channel.exchangeDelete(exchange.getName());
        }

        for (Object obj : bindingList) {
            Binding binding = JSON.parseObject(JSON.toJSONString(obj), Binding.class);
            Channel channel = getChannel(binding.getVhost());
            channel.queueUnbind(
                    binding.getDestination(),
                    binding.getSource(),
                    binding.getRouting_key()
            );
        }
    }

    /**
     * 关闭连接
     *
     * @throws IOException ...
     */
    public static void close() throws IOException {
        for (Connection connection : connectionList) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        parseJson();
        createChannelList();
        delete();
        creteQueues();
        createExchanges();
        createBindings();
        close();
    }
}