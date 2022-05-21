package com.federation.exchange;

import java.util.ArrayList;
import java.util.List;

/**
 * Producer:
 *
 * @author sunchen
 * @date 2021/9/12 2:44 上午
 */
public class Producer {
    /**
     * 绑定键
     */
    static List<String> routingKeyKeys = new ArrayList<String>(){
        {
            add("key:study:federations:shanghai:exchange");
            add("key:study:federations:beijing:exchange");
        }
    };

    /**
     * 绑定键轮询值
     */
    static int routingKeyIndex = 0;

    /**
     * 获取routeKey
     * @return String 绑定键
     */
    public static String getRoutingKey() {
        String routeKey = routingKeyKeys.get(routingKeyIndex);
        routingKeyIndex = (routingKeyIndex + 1) % routingKeyKeys.size();
        return routeKey;
    }
}