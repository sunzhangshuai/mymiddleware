package com.confirm;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/20 11:00 下午
 */
public class Producer {

    /**
     * 发送消息
     *
     * @param channel 信道
     * @throws IOException ...
     */
    public void publish(Channel channel) throws IOException, InterruptedException {
    }

    ;

    public void run() throws IOException, TimeoutException, InterruptedException {
        // 1. 获取连接
        Connection connection = MqConnect.connection();
        assert connection != null;

        // 2. 获取信道
        Channel channel = connection.createChannel();


        // 3. 声明交换器、声明队列、绑定交换器和队列
        channel.exchangeDeclare(Config.exchangeName, Config.typeDirect, true);
        channel.queueDeclare(Config.queueName, true, false, false, null);
        channel.queueBind(Config.queueName, Config.exchangeName, Config.routingKey);

        publish(channel);
    }

    public static void switchRun(String producer) throws IOException, TimeoutException, InterruptedException {
        switch (producer) {
            case "tx":
                new TxConfirm().run();
                break;
            case "sync":
                new SyncConfirm().run();
                break;
            case "batchSync":
                new BatchSyncConfirm().run();
                break;
            case "async":
                new AsyncConfirm().run();
                break;
            default:
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //switchRun("tx");
        // switchRun("sync");
        //switchRun("batchSync");
        switchRun("async");
    }
}