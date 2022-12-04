package com.gmall.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 定义拦截器,解决数据漂移(零点)问题和不同的表数据发往不同的路径
 */
public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1、获取header和body当中的数据
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        // 2、解析log当中的ts和table字段
        JSONObject jsonObject = JSONObject.parseObject(log);
        String table = jsonObject.getString("table");
        String ts = jsonObject.getString("ts");

        // 3、把ts和table放到header当中的tableName和timestamp
        headers.put("tableName", table);
        headers.put("timestamp", ts + "000");
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
