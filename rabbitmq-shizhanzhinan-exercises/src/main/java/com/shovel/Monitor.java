package com.shovel;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.MqConnect;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.util.HttpUtil;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Monitor:
 *
 * @author sunchen
 * @date 2021/9/20 12:29 上午
 */
public class Monitor {
    /**
     * 消息确认方式
     */
    static String ackMode = "no-ack";

    /**
     * 消息传输出去的shovel
     */
    static String shovelNameOut = "study:shovel:out";

    /**
     * 消息传输回来的shovel
     */
    static String shovelNameIn = "study:shovel:in";

    /**
     * 协议版本
     */
    static String protocol = "amqp091";

    /**
     * 消息到达多少时应该出去
     */
    static int msgOutNum = 3000000;

    /**
     * 消息到达多少时应该回来
     */
    static int msgInNum = 500000;

    /**
     * running状态的shovel link
     */
    static Set<String> shovelStatusSet;

    /**
     * 获取shovel的连接
     *
     * @return String
     */
    public static String getShovelUri() throws UnsupportedEncodingException {
        return String.format("%s://%s:%s@%s:%s/%s",
                MqConnect.protocol,
                MqConnect.originUsername,
                MqConnect.originPassword,
                MqConnect.ip,
                MqConnect.port,
                URLEncoder.encode(MqConnect.virtualHost, "UTF-8")
        );
    }

    /**
     * 创建 shovel 连接
     *
     * @param name        String 连接名称
     * @param sourceQueue String 源队列
     * @param destQueue   String 目标队列
     * @throws UnsupportedEncodingException ...
     */
    public static void createShovelLink(String name, String sourceQueue, String destQueue) throws UnsupportedEncodingException {
        System.out.println("shovel:" + name + "开始开启");
        if (shovelStatusSet.contains(name)) {
            System.out.println("shovel:" + name + "开启了");
            return;
        }
        String url = String.format("http://localhost:15672/api/parameters/shovel/%s/%s",
                URLEncoder.encode(MqConnect.virtualHost, "UTF-8"),
                name
        );
        JSONObject params = new JSONObject();
        params.put("component", "shovel");
        params.put("vhost", MqConnect.virtualHost);
        params.put("name", name);
        JSONObject value = new JSONObject();
        value.put("src-uri", getShovelUri());
        value.put("dest-uri", getShovelUri());
        value.put("src-protocol", protocol);
        value.put("dest-protocol", protocol);
        value.put("ack-mode", ackMode);
        value.put("src-queue", sourceQueue);
        value.put("src-delete-after", "never");
        value.put("dest-queue", destQueue);
        value.put("dest-add-forward-headers", false);
        params.put("value", value);
        String result = HttpUtil.doPut(url, JSON.toJSONString(params), MqConnect.originUsername, MqConnect.originPassword);
        System.out.println("result:" + result);
        shovelStatusSet.add(name);
        System.out.println("shovel:" + name + "开启了");
    }

    /**
     * 断开shovel link
     *
     * @param name shovel 名称
     * @throws UnsupportedEncodingException ...
     */
    public static void deleteShovelLink(String name) throws UnsupportedEncodingException {
        System.out.println("shovel:" + name + "开始关闭");
        if (!shovelStatusSet.contains(name)) {
            System.out.println("shovel:" + name + "关闭了");
            return;
        }
        String url = String.format("http://localhost:15672/api/shovels/vhost/%s/%s",
                URLEncoder.encode(MqConnect.virtualHost, "UTF-8"),
                name
        );
        String result = HttpUtil.doDelete(url, MqConnect.originUsername, MqConnect.originPassword);
        System.out.println(result);
        System.out.println("shovel:" + name + "关闭了");
        shovelStatusSet.remove(name);
    }

    /**
     * 初始化shovel link 状态
     *
     * @throws UnsupportedEncodingException ...
     */
    public static void shovelLinkStatus() throws UnsupportedEncodingException {
        String url = String.format("http://localhost:15672/api/shovels/%s",
                URLEncoder.encode(MqConnect.virtualHost, "UTF-8")
        );
        String result = HttpUtil.doGet(url, MqConnect.originUsername, MqConnect.originPassword);
        List<JSONObject> jsonArrays = JSON.parseArray(result, JSONObject.class);
        shovelStatusSet = jsonArrays.stream()
                .filter(item -> (item.getString("name").equals(shovelNameIn)
                        || item.getString("name").equals(shovelNameOut)) && "running".equals(item.getString("state")))
                .map(item -> item.getString("name"))
                .collect(Collectors.toSet());
        System.out.println(shovelStatusSet);
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();
        // 初始化shovelLink状态
        shovelLinkStatus();
        try {
            while (true) {
                Thread.sleep(1000);
                // 1. 获取消息个数
                DeclareOk declareOk = channel.queueDeclarePassive(Producer.sourceQueueName);
                int messageCount = declareOk.getMessageCount();
                System.out.println("消息个数：" + messageCount);
                if (messageCount == 0) {
                    break;
                } else if (messageCount > msgOutNum) {
                    // 消息个数堆积达到msgOutNum，消息转发出去
                    createShovelLink(shovelNameOut, Producer.sourceQueueName, Producer.destQueueName);
                    deleteShovelLink(shovelNameIn);
                } else if (messageCount < msgInNum) {
                    // 消息个数降到msgInNum，将转出去的消息转回来
                    deleteShovelLink(shovelNameOut);
                    createShovelLink(shovelNameIn, Producer.destQueueName, Producer.sourceQueueName);
                }
            }
        } finally {
            deleteShovelLink(shovelNameOut);
            deleteShovelLink(shovelNameIn);
            channel.close();
            connection.close();
        }
    }
}