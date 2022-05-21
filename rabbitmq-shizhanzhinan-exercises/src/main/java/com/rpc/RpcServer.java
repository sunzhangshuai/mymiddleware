package com.rpc;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.CommonConsumer;
import com.common.MqConnect;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import com.rabbitmq.client.Envelope;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


/**
 * RpcServer:
 *
 * @author sunchen
 * @date 2021/6/15 11:30 下午
 */
public class RpcServer {
    public static void main(String[] args) throws IOException, TimeoutException {
        //1. 获取连接和信道
        Connection connection = MqConnect.connection();
        Channel channel = connection.createChannel();
        //1. 声明回调队列
        channel.basicConsume(RpcClient.queueName, new CommonConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String correlationId = properties.getCorrelationId();
                String msg = new String(body);
                JSONObject jsonObject = JSON.parseObject(msg);
                int sum = jsonObject.getIntValue("a") + jsonObject.getIntValue("b");
                String replyTo = properties.getReplyTo();
                AMQP.BasicProperties basicProperties = new AMQP.BasicProperties()
                        .builder()
                        .correlationId(correlationId)
                        .build();
                channel.basicPublish("", replyTo, basicProperties, String.valueOf(sum).getBytes(StandardCharsets.UTF_8));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });
    }
}
