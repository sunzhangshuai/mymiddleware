package com.confirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * AsyncConfirm:
 *
 * @author sunchen
 * @date 2021/6/20 10:57 下午
 */
public class AsyncConfirm extends Producer {

    private final SortedSet<Long> unconfirmedSet = new TreeSet<>();

    @Override
    public void publish(Channel channel) throws IOException, InterruptedException {
        // 开启确认机制
        channel.confirmSelect();
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliverTag, boolean multiple) throws IOException {
                System.out.println("ack success: deliverTag =" + deliverTag + ", multiple : " + multiple);
                if (multiple) {
                    unconfirmedSet.headSet(deliverTag + 1L).clear();
                } else {
                    unconfirmedSet.remove(deliverTag);
                }

            }

            @Override
            public void handleNack(long deliverTag, boolean multiple) throws IOException {
                if (multiple) {
                    // TODO 获取要重发的 deliverTag
                    unconfirmedSet.headSet(deliverTag + 1L).clear();
                } else {
                    // TODO 获取要重发的 deliverTag
                    unconfirmedSet.remove(deliverTag);
                }
                //重发
                System.out.println("重发");
            }
        });
        for (int i = 0; i < 100; i++) {
            long nextPublishSeqNo = channel.getNextPublishSeqNo();
            System.out.println("nextPublishSeqNo=" + nextPublishSeqNo);
            channel.basicPublish(Config.exchangeName,
                    Config.routingKey,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    ("async" + i).getBytes(StandardCharsets.UTF_8));
            Thread.sleep(10);
            unconfirmedSet.add(nextPublishSeqNo);
        }
    }
}