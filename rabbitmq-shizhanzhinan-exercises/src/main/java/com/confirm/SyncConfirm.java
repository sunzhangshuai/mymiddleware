package com.confirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Sync:
 *
 * @author sunchen
 * @date 2021/6/20 10:56 下午
 */
public class SyncConfirm extends Producer {
    @Override
    public void publish(Channel channel) throws IOException {
        // 开启确认机制
        channel.confirmSelect();
        try {
            channel.basicPublish(Config.exchangeName,
                    Config.routingKey,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    "sync".getBytes(StandardCharsets.UTF_8));
            if (!channel.waitForConfirms()) {
                System.out.println("send fail");
                return;
            }
            System.out.println("send success");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}