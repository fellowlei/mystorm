package com.mark.storm.mapreduce;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by fellowlei on 2018/3/4
 */
public class ReduceBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    static Map<String,Integer> map = new HashMap<>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");
        Integer sum = 0;
        if(map.containsKey(word)){
            sum = count + map.get(word);
            map.put(word,sum);
        }else{
            map.put(word,count);
            sum = count;
        }
        outputCollector.emit(new Values(word,sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","sum"));
    }
}
