package com.mark.storm.mapreduce.service;

import java.util.HashMap;
import java.util.Map;

public class RedisService {
    static Map<String,String> map = new HashMap<>();
    static {
        map.put("1","mark1");
        map.put("2","mark2");
        map.put("3","mark3");
        map.put("4","mark4");
        map.put("5","mark5");

    }
    public String query(String key){
        return map.get(key);
    }

    public void set(String key,String value){
        map.put(key,value);
    }
}
