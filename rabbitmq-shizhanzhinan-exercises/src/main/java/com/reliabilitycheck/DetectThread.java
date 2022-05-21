package com.reliabilitycheck;

import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * DetectThread:
 *
 * @author sunchen
 * @date 2021/9/25 9:58 下午
 */
public class DetectThread implements Runnable {
    private final HashMap<String, Integer> map;

    public DetectThread(HashMap<String, Integer> map) {
        this.map = map;
    }

    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            TimeUnit.MINUTES.sleep(Check.intervalTime);
            long currentTime = System.currentTimeMillis();
            synchronized (map) {
                if (map.size() > 0) {
                    for (String message : map.keySet()) {
                        long messageTime = parseTime(message);
                        if (currentTime - messageTime > (long) Check.intervalTime * 60 * 1000) {
                            System.out.println("-----------message lose----------");
                            map.remove(message);
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取消息时间
     *
     * @param message 消息
     * @return 消息时间
     */
    public long parseTime(String message) {
        return Long.parseLong(message.split("-")[0]);
    }
}