package com.cluster.metaTransfer;

import com.alibaba.fastjson.JSON;
import com.cluster.common.Binding;
import com.cluster.common.Exchange;
import com.cluster.common.Queue;
import com.common.MqConnect;
import com.util.HttpUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * TransferWithHttp: 通过http方式创建队列
 *
 * @author sunchen
 * @date 2021/7/31 11:05 下午
 */
public class TransferWithHttp extends Transfer {
    /**
     * 创建队列
     *
     * @throws UnsupportedEncodingException ...
     */
    public static void creteQueues() throws UnsupportedEncodingException {
        for (Object obj : queueList) {
            Queue queue = JSON.parseObject(JSON.toJSONString(obj), Queue.class);
            String url = String.format("http://%s:15672/api/queues/%s/%s",
                    ip,
                    URLEncoder.encode(queue.getVhost(), "UTF-8"),
                    URLEncoder.encode(queue.getName(), "UTF-8")
            );
            Map<String, Object> param = new HashMap<>(4);
            param.put("auto_delete", queue.getAuto_delete());
            param.put("durable", queue.getDurable());
            param.put("arguments", queue.getArguments());
            Collections.shuffle(nodes);
            String node = nodes.get(0);
            param.put("node", node);
            String jsonObject = HttpUtil.doPut(url, JSON.toJSONString(param), MqConnect.originUsername, MqConnect.originPassword);
            System.out.println("queue:" + jsonObject);
        }
    }

    /**
     * 创建交换器
     *
     * @throws UnsupportedEncodingException ...
     */
    public static void createExchanges() throws UnsupportedEncodingException {
        for (Object obj : exchangeList) {
            Exchange exchange = JSON.parseObject(JSON.toJSONString(obj), Exchange.class);
            String url = String.format("http://%s:15672/api/exchanges/%s/%s", ip,
                    URLEncoder.encode(exchange.getVhost(), "UTF-8"),
                    URLEncoder.encode(exchange.getName(), "UTF-8")
            );
            Map<String, Object> param = new HashMap<>(6);
            param.put("auto_delete", exchange.getAuto_delete());
            param.put("durable", exchange.getDurable());
            param.put("arguments", exchange.getArguments());
            param.put("internal", exchange.getInternal());
            param.put("type", exchange.getType());
            Collections.shuffle(nodes);
            String node = nodes.get(0);
            param.put("node", node);
            String result = HttpUtil.doPut(url, JSON.toJSONString(param), MqConnect.originUsername, MqConnect.originPassword);
            System.out.println("exchange:" + result);
        }
    }

    /**
     * 创建绑定键
     *
     * @throws UnsupportedEncodingException ...
     */
    public static void createBindings() throws UnsupportedEncodingException {
        String url;
        for (Object obj : bindingList) {
            Map<String, Object> param = new HashMap<>(3);
            Binding binding = JSON.parseObject(JSON.toJSONString(obj), Binding.class);
            if ("queue".equals(binding.getDestination_type())) {
                url = String.format("http://%s:15672/api/bindings/%s/e/%s/q/%s",
                        ip,
                        URLEncoder.encode(binding.getVhost(), "UTF-8"),
                        URLEncoder.encode(binding.getSource(), "UTF-8"),
                        URLEncoder.encode(binding.getDestination(), "UTF-8")
                );
                param.put("arguments", binding.getArguments());
                param.put("routing_key", binding.getRouting_key());
                Collections.shuffle(nodes);
                String node = nodes.get(0);
                param.put("node", node);
            } else {
                url = String.format("http://%s:15672/api/bindings/%s/e/%s/e/%s", ip,
                        URLEncoder.encode(binding.getVhost(), "UTF-8"),
                        URLEncoder.encode(binding.getSource(), "UTF-8"),
                        URLEncoder.encode(binding.getDestination(), "UTF-8")
                );
                param.put("arguments", binding.getArguments());
                param.put("routing_key", binding.getRouting_key());
                Collections.shuffle(nodes);
                String node = nodes.get(0);
                param.put("node", node);
            }
            String result = HttpUtil.doPost(url, JSON.toJSONString(param), MqConnect.originUsername, MqConnect.originPassword);
            System.out.println("binding:" + result);
        }
    }

    /**
     * 删除
     *
     * @throws UnsupportedEncodingException ...
     */
    public static void delete() throws UnsupportedEncodingException {
        for (Object obj : bindingList) {
            String url;
            Binding binding = JSON.parseObject(JSON.toJSONString(obj), Binding.class);
            if ("queue".equals(binding.getDestination_type())) {
                url = String.format("http://%s:15672/api/bindings/%s/e/%s/q/%s/%s",
                        ip,
                        URLEncoder.encode(binding.getVhost(), "UTF-8"),
                        URLEncoder.encode(binding.getSource(), "UTF-8"),
                        URLEncoder.encode(binding.getDestination(), "UTF-8"),
                        URLEncoder.encode(binding.getRouting_key(), "UTF-8")
                );
            } else {
                url = String.format("http://%s:15672/api/bindings/%s/e/%s/e/%s/%s", ip,
                        URLEncoder.encode(binding.getVhost(), "UTF-8"),
                        URLEncoder.encode(binding.getSource(), "UTF-8"),
                        URLEncoder.encode(binding.getDestination(), "UTF-8"),
                        URLEncoder.encode(binding.getRouting_key(), "UTF-8")
                );
            }
            System.out.println("binding:" + url);
            String result = HttpUtil.doDelete(url, MqConnect.originUsername, MqConnect.originPassword);
            System.out.println("binding:" + result);
        }

        for (Object obj : queueList) {
            Queue queue = JSON.parseObject(JSON.toJSONString(obj), Queue.class);
            String url = String.format("http://%s:15672/api/queues/%s/%s",
                    ip,
                    URLEncoder.encode(queue.getVhost(), "UTF-8"),
                    URLEncoder.encode(queue.getName(), "UTF-8")
            );
            System.out.println("queue:" + url);
            String result = HttpUtil.doDelete(url, MqConnect.originUsername, MqConnect.originPassword);
            System.out.println("queue:" + result);
        }

        for (Object obj : exchangeList) {
            Exchange exchange = JSON.parseObject(JSON.toJSONString(obj), Exchange.class);
            String url = String.format("http://%s:15672/api/exchanges/%s/%s", ip,
                    URLEncoder.encode(exchange.getVhost(), "UTF-8"),
                    URLEncoder.encode(exchange.getName(), "UTF-8")
            );
            System.out.println("exchange:" + url);
            String result = HttpUtil.doDelete(url, MqConnect.originUsername, MqConnect.originPassword);
            System.out.println("exchange:" + result);
        }
    }

    public static void main(String[] args) throws IOException {
        parseJson();
        delete();
        creteQueues();
        createExchanges();
        createBindings();
    }
}