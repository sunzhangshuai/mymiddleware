package com.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.Arrays;

/**
 * MqConsumer:
 *
 * @author sunchen
 * @date 2021/6/14 1:11 下午
 */
public class CommonConsumer extends DefaultConsumer {

    public CommonConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleConsumeOk(String consumerTag) {
        System.out.println("handleConsumeOk consumerTag:" + consumerTag);
        super.handleConsumeOk(consumerTag);
    }

    /**
     * No-op implementation of {@link Consumer#handleCancelOk}.
     * @param consumerTag the defined consumer tag (client- or server-generated)
     */
    @Override
    public void handleCancelOk(String consumerTag) {
        System.out.println("handleCancelOk consumerTag:" + consumerTag);
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        System.out.println("handleCancel consumerTag:" + consumerTag);
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        System.out.println("handleShutdownSignal consumerTag:" + consumerTag);
        System.out.println("handleShutdownSignal sig:" + sig);
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
        System.out.println("handleRecoverOk consumerTag:" + consumerTag);
    }

    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body)
            throws IOException
    {
       System.out.println("handleDelivery body:" + new String(body));
    }
}