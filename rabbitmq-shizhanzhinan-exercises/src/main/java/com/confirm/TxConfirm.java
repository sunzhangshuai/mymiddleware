package com.confirm;

import com.common.MqConnect;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/6/2 9:10 下午
 */
public class TxConfirm extends Producer {

    @Override
    public void publish(Channel channel) throws IOException {
        // 开启事务
        channel.txSelect();
        try {
            channel.basicPublish(Config.exchangeName,
                    Config.routingKey,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    "tx".getBytes(StandardCharsets.UTF_8));
            channel.txCommit();
        } catch (Exception e) {
            e.printStackTrace();
            channel.txRollback();
        }
    }
}
