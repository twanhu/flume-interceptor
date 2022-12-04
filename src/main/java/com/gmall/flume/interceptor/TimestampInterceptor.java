package com.gmall.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 拦截器 : 定义在source上,解决数据漂移(零点)问题
 *
 * event : flume中处理消息的基本单元，由header和body组成
 * Header : 是 key/value 形式的，可以用来制造路由决策或携带其他结构化信息(如事件的时间戳或事件来源的服务器主机名)
 * Body : 一个字节数组，包含了实际的内容
 */
public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1、获取header和body当中的数据
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        // 2、解析log（json）的ts时间戳字段
        JSONObject jsonObject = JSONObject.parseObject(log);
        String ts = jsonObject.getString("ts");
        // 3、把解析出来的ts put到header头当中的timestamp
        headers.put("timestamp", ts);
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
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
