package com.confirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * BatchSyncConfirm:
 *
 * @author sunchen
 * @date 2021/6/20 10:56 下午
 */
public class BatchSyncConfirm extends Producer {
    int maxCount = 10;

    @Override
    public void publish(Channel channel) throws IOException {
        // 开启确认机制
        channel.confirmSelect();
        try {
            int count = 0;
            while (true) {
                channel.basicPublish(Config.exchangeName,
                        Config.routingKey,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        ("sync" + count).getBytes(StandardCharsets.UTF_8));
                count++;
                if (count % maxCount == 0) {
                    if (!channel.waitForConfirms()) {
                        System.out.println("fail");
                    } else {
                        System.out.println("success");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}