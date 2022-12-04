package com.gmall.flume.utils;

import com.alibaba.fastjson.JSONObject;

public class JSONUtil {

    // 通过异常捕捉，校验json是不是一个合法的json
    public static boolean isJSONValidate(String log) {
        // 将数据转换成JSON对象，如果能转换则是合法的json数据，如果异常则是非法json数据
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
