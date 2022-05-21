package com.cluster.monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.MqConnect;
import com.util.HttpUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Monitor:
 *
 * @author sunchen
 * @date 2021/9/21 6:12 下午
 */
public class Monitor {
    public static void main(String[] args) throws UnsupportedEncodingException {
        System.out.println("=======================节点信息如下========================");
        System.out.println(getClusterData());
        System.out.println("=======================交换器信息如下========================");
        System.out.println(getExchangeData(Producer.exchangeName));
    }

    /**
     * 获取节点数据
     * @return List<ClusterNode>
     */
    public static List<ClusterNode> getClusterData() {
        String url = "http://localhost:15672/api/nodes";
        String result = HttpUtil.doGet(url, MqConnect.originUsername, MqConnect.originPassword);
        return JSON.parseArray(result, ClusterNode.class);
    }

    /**
     * 获取交换器数据
     * @param exchangeNames ArrayList<String> 交换器列表
     * @return List<Exchange>
     * @throws UnsupportedEncodingException ...
     */
    public static List<Exchange> getExchangeData (String... exchangeNames) throws UnsupportedEncodingException {
        List<Exchange> exchanges = new ArrayList<>();
        for (String exchangeName : exchangeNames) {
            String url = "http://localhost:15672/api/exchanges/%s/%s";
            url = String.format(url,
                    URLEncoder.encode(MqConnect.virtualHost, "UTF-8"), exchangeName);
            String result = HttpUtil.doGet(url, MqConnect.originUsername, MqConnect.originPassword);
            System.out.println(result);
            JSONObject jsonObject = JSON.parseObject(result, JSONObject.class);
            if (jsonObject.getJSONObject("message_stats") == null) {
                continue;
            }
            Exchange exchange = new Exchange();
            exchange.setPublishIn(jsonObject.getJSONObject("message_stats").getLong("publish_in"));
            exchange.setPublishInRate(jsonObject.getJSONObject("message_stats").getJSONObject("publish_in_details").getDouble("rate"));
            exchange.setPublishOut(jsonObject.getJSONObject("message_stats").getLong("publish_out"));
            exchange.setPublishInRate(jsonObject.getJSONObject("message_stats").getJSONObject("publish_out_details").getDouble("rate"));
            exchanges.add(exchange);
        }
        return exchanges;
    }
}