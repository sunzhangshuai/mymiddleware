package com.cluster.metaTransfer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.util.JsonUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transfer:
 *
 * @author sunchen
 * @date 2021/8/1 2:40 上午
 */
public class Transfer {
    /**
     * 队列列表
     */
    static JSONArray queueList;

    /**
     * 交换器列表
     */
    static JSONArray exchangeList;

    /**
     * 绑定键列表
     */
    static JSONArray bindingList;

    static JSONArray vhosts;

    /**
     * 调用ip
     */
    static String ip = "192.168.0.105";

    /**
     * 集群节点
     */
    static List<String> nodes = new ArrayList<String>() {
        {
            add("rabbit1@zhangshuai24deMacBook-Pro");
            add("rabbit2@zhangshuai24deMacBook-Pro");
            add("rabbit3@zhangshuai24deMacBook-Pro");
        }
    };

    static List<Integer> ports = new ArrayList<Integer>() {
        {
            add(5672);
            add(5673);
            add(5674);
        }
    };

    /**
     * 格式化json数据
     *
     * @throws IOException ...
     */
    public static void parseJson() throws IOException {
        //1. 读取json文件
        JSONObject config = JsonUtil.readJsonFromClassPath("mq.json");
        queueList = config.getJSONArray("queues");
        bindingList = config.getJSONArray("bindings");
        exchangeList = config.getJSONArray("exchanges");
        vhosts = config.getJSONArray("vhosts");
    }
}