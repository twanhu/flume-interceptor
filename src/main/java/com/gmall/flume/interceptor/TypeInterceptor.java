package com.gmall.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    // 声明一个存放事件的集合
    private List<Event> addHeaderEvenst;

    @Override
    public void initialize() {
        // 初始化存放事件的集合
        addHeaderEvenst = new ArrayList<>();
    }

    // 单个事件拦截
    @Override
    public Event intercept(Event event) {

        // 1、获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        // 2、获取事件中的body信息
        String body = new String(event.getBody());

        // 3、根据body中是否有"hello"来决定添加怎样的头信息
        if (body.contains("hello")) {
            // 4、添加头信息
            headers.put("type","first");
        } else {
            // 4、添加头信息
            headers.put("type","second");
        }

        return event;
    }

    // 批量事件拦截
    @Override
    public List<Event> intercept(List<Event> events) {

        // 1、清空集合
        addHeaderEvenst.clear();

        // 2、遍历events
        for (Event event : events) {
            // 3、给每一个事件添加头信息
            addHeaderEvenst.add(intercept(event));
        }

        // 4、返回结果
        return addHeaderEvenst;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
