package com.gmall.flume.interceptor;

import com.gmall.flume.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 拦截器 : 定义在source上,拦截不合法的json数据
 *
 * event : flume中处理消息的基本单元，由header和body组成
 * Header : 是 key/value 形式的，可以用来制造路由决策或携带其他结构化信息(如事件的时间戳或事件来源的服务器主机名)
 * Body : 一个字节数组，包含了实际的内容
 */
public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1、获取body当中的数据
        byte[] body = event.getBody();
        // 通过使用指定的字符集对指定的字节数组进行解码，构造新的字符串
        String log = new String(body, StandardCharsets.UTF_8);
        // 2、判断是不是合法的json，是：return event，不是：return null
        if (JSONUtil.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        /**
         * 在调用Iterator的next()方法之前，迭代器的索引位于第一个元素之前，不指向任何元素
         */
        // iterator() : 获取Iterator对象
        Iterator<Event> iterator = list.iterator();
        // hasNext() : 判断迭代器中是否存在下一个元素
        while (iterator.hasNext()) {
            // next() : 返回迭代器的下一个元素
            Event event = iterator.next();
            if (intercept(event) == null) {
                // 移除此集合中不合法的json字符串
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
